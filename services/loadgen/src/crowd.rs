//! The set of virtual customers, resizable while the process runs.

use std::time::Duration;

use tokio::task::JoinHandle;

/// Newcomers arrive one by one: a hundred customers opening the shop in the
/// same millisecond is a different experiment from "the shop got popular".
const ARRIVAL_GAP: Duration = Duration::from_millis(100);

pub type SpawnUser = Box<dyn Fn(Duration) -> JoinHandle<()> + Send>;

pub struct Crowd {
    spawn_user: SpawnUser,
    users: Vec<JoinHandle<()>>,
}

impl Crowd {
    pub fn new(spawn_user: SpawnUser) -> Self {
        Self {
            spawn_user,
            users: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.users.len()
    }

    pub fn resize(&mut self, target: usize) {
        for user in self.users.drain(target.min(self.users.len())..) {
            user.abort();
        }
        let newcomers = target - self.users.len();
        for n in 0..newcomers {
            let delay = ARRIVAL_GAP * n as u32;
            self.users.push((self.spawn_user)(delay));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts live users the way the online gauge does: up on start, down on drop.
    struct Alive(Arc<AtomicUsize>);

    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn counting_crowd() -> (Crowd, Arc<AtomicUsize>) {
        let alive = Arc::new(AtomicUsize::new(0));
        let counter = alive.clone();
        let crowd = Crowd::new(Box::new(move |_delay| {
            counter.fetch_add(1, Ordering::SeqCst);
            let guard = Alive(counter.clone());
            tokio::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
        }));
        (crowd, alive)
    }

    #[tokio::test]
    async fn grows_to_the_target() {
        let (mut crowd, alive) = counting_crowd();
        crowd.resize(5);
        assert_eq!(crowd.size(), 5);
        assert_eq!(alive.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn shrinking_stops_the_extra_users() {
        let (mut crowd, alive) = counting_crowd();
        crowd.resize(5);
        crowd.resize(2);
        tokio::task::yield_now().await;
        assert_eq!(crowd.size(), 2);
        assert_eq!(alive.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn resizing_to_the_same_size_keeps_everyone() {
        let (mut crowd, alive) = counting_crowd();
        crowd.resize(3);
        crowd.resize(3);
        assert_eq!(alive.load(Ordering::SeqCst), 3);
    }
}
