#[cfg(test)]
#[macro_use]
extern crate assert_matches;

mod cht;

use std::{
  any::Any,
  collections::hash_map::RandomState,
  hash::{BuildHasher, Hash},
  sync::Arc,
};

use parking_lot::RwLock;

const WAITER_MAP_NUM_SEGMENTS: usize = 64;

#[derive(Debug)]
pub enum InitResult<V, E> {
  Initialized(V),
  ReadExisting(V),
  InitErr(Arc<E>),
}

type ErrorObject = Arc<dyn Any + Send + Sync + 'static>;
type WaiterValue<V> = Option<Result<V, ErrorObject>>;
type Waiter<V> = Arc<RwLock<WaiterValue<V>>>;

pub struct ConcurrentInitializer<K, V, S = RandomState> {
  waiters: crate::cht::SegmentedHashMap<Arc<K>, Waiter<V>, S>,
}

impl<K, V> ConcurrentInitializer<K, V>
where
  K: Eq + Hash,
  V: Clone,
{
  pub fn new() -> Self {
    Self::with_hasher(RandomState::new())
  }
}

impl<K, V, S> ConcurrentInitializer<K, V, S>
where
  K: Eq + Hash,
  V: Clone,
  S: BuildHasher,
{
  pub fn with_hasher(build_hasher: S) -> Self {
    Self {
      waiters: cht::SegmentedHashMap::with_num_segments_and_hasher(
        WAITER_MAP_NUM_SEGMENTS,
        build_hasher,
      ),
    }
  }

  /// # Panics
  /// Panics if the `init` closure has been panicked.
  pub fn try_get_or_init<E>(
    &self,
    key: &Arc<K>,
    // Closure to get an existing value from somewhere.
    mut get: impl FnMut() -> Result<Option<V>, E>,
    // Closure to initialize a new value.
    init: impl FnOnce() -> Result<V, E>,
  ) -> InitResult<V, E>
  where
    E: Send + Sync + 'static,
  {
    use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

    use InitResult::*;

    const MAX_RETRIES: usize = 200;
    let mut retries = 0;

    let (cht_key, hash) = self.cht_key_hash(key);

    loop {
      let waiter = Arc::new(RwLock::new(None));
      let mut lock = waiter.write();

      match self.try_insert_waiter(cht_key.clone(), hash, &waiter) {
        None => {
          // Our waiter was inserted.
          // Check if the value has already been inserted by other thread.
          match get() {
            Ok(ok) => {
              if let Some(value) = ok {
                // Yes. Set the waiter value, remove our waiter, and return
                // the existing value.
                *lock = Some(Ok(value.clone()));
                self.remove_waiter(cht_key, hash);
                return InitResult::ReadExisting(value);
              }
            }
            Err(err) => {
              // Error. Set the waiter value, remove our waiter, and return
              // the error.
              let err: ErrorObject = Arc::new(err);
              *lock = Some(Err(Arc::clone(&err)));
              self.remove_waiter(cht_key, hash);
              return InitErr(err.downcast().unwrap());
            }
          }

          // The value still does not exist. Let's evaluate the init
          // closure. Catching panic is safe here as we do not try to
          // evaluate the closure again.
          match catch_unwind(AssertUnwindSafe(init)) {
            // Evaluated.
            Ok(value) => {
              let (waiter_val, init_res) = match value {
                Ok(value) => (Some(Ok(value.clone())), InitResult::Initialized(value)),
                Err(e) => {
                  let err: ErrorObject = Arc::new(e);
                  (Some(Err(Arc::clone(&err))), InitResult::InitErr(err.downcast().unwrap()))
                }
              };
              *lock = waiter_val;
              self.remove_waiter(cht_key, hash);
              return init_res;
            }
            // Panicked.
            Err(payload) => {
              *lock = None;
              // Remove the waiter so that others can retry.
              self.remove_waiter(cht_key, hash);
              resume_unwind(payload);
            }
          } // The write lock will be unlocked here.
        }
        Some(res) => {
          // Somebody else's waiter already exists. Drop our write lock and
          // wait for the read lock to become available.
          std::mem::drop(lock);
          match &*res.read() {
            Some(Ok(value)) => return ReadExisting(value.clone()),
            Some(Err(e)) => return InitErr(Arc::clone(e).downcast().unwrap()),
            // None means somebody else's init closure has been panicked.
            None => {
              retries += 1;
              if retries < MAX_RETRIES {
                // Retry from the beginning.
                continue;
              } else {
                panic!(
                  "Too many retries. Tried to read the return value from the `init` \
                                closure but failed {} times. Maybe the `init` kept panicking?",
                  retries
                );
              }
            }
          }
        }
      }
    }
  }

  #[inline]
  fn remove_waiter(&self, cht_key: Arc<K>, hash: u64) {
    self.waiters.remove(hash, |k| k == &cht_key);
  }

  #[inline]
  fn try_insert_waiter(&self, cht_key: Arc<K>, hash: u64, waiter: &Waiter<V>) -> Option<Waiter<V>> {
    let waiter = Arc::clone(waiter);
    self.waiters.insert_if_not_present(cht_key, hash, waiter)
  }

  #[inline]
  fn cht_key_hash(&self, key: &Arc<K>) -> (Arc<K>, u64) {
    let cht_key = Arc::clone(key);
    let hash = self.waiters.hash(&cht_key);
    (cht_key, hash)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
  };

  use super::*;

  #[test]
  fn test_concurrent() {
    let initializer: Arc<ConcurrentInitializer<String, u64>> =
      Arc::new(ConcurrentInitializer::new());
    let store = Arc::new(AtomicUsize::new(0));

    // Spawn four threads.
    let threads: Vec<_> = (0..16_u8)
      .map(|thread_id| {
        let my_initializer = initializer.clone();
        let my_store = store.clone();

        thread::spawn(move || {
          println!("Thread {} started.", thread_id);

          // Try to insert and get the value for key1. Although all four
          // threads will call `try_get_or_init` at the same time,
          // the init closure must be called only once.
          let value: InitResult<u64, std::io::Error> = my_initializer.try_get_or_init(
            &Arc::new("key1".to_owned()),
            || {
              let size = my_store.load(Ordering::SeqCst) as u64;
              if size > 0 {
                return Ok(Some(size));
              } else {
                return Ok(None);
              }
            },
            || {
              println!("The init closure called by thread {}.", thread_id);
              let size = std::fs::metadata("./Cargo.toml")?.len();
              my_store.store(size as usize, Ordering::SeqCst);
              Ok(size)
            },
          );

          // Ensure the value exists now.
          assert_matches!(value, InitResult::Initialized(_) | InitResult::ReadExisting(_));

          println!("Thread {} got the value. (len: {:?})", thread_id, value);
        })
      })
      .collect();

    // Wait all threads to complete.
    threads.into_iter().for_each(|t| t.join().expect("Thread failed"));
  }
}
