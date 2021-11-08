use crossbeam::channel::{unbounded, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::types::{NaiveError, Result};

pub struct ThreadPool {
    workers: Vec<thread::JoinHandle<()>>,
    sender: Option<Sender<Box<dyn FnOnce() + Send + 'static>>>,
}

impl ThreadPool {
    pub fn new(num: usize) -> Self {
        let (sender, receiver) = unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        let mut workers = Vec::with_capacity(num);
        for _ in 0..num {
            let receiver = receiver.clone();
            workers.push(thread::spawn(move || {
                // Repeatedly pick a task from the channel until the channel is closed.
                while let Ok(task) = receiver.recv() {
                    task();
                }
            }));
        }
        Self {
            workers,
            sender: Some(sender),
        }
    }

    pub fn add_task<F>(&self, task: F) -> Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        Ok(self.sender.as_ref().unwrap().send(Box::new(task))?)
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Drop the channel and then join each worker.
        self.sender.take();
        while let Some(worker) = self.workers.pop() {
            worker.join().expect("Unable to join a worker thread.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_pool() {
        const NUM_RUNS: usize = 100;
        for _ in 0..NUM_RUNS {
            let sum = Arc::new(Mutex::new(0));
            {
                let thread_pool = ThreadPool::new(5);
                thread::sleep(Duration::from_micros(10u64)); // Make workers wait.
                for i in 1..=100 {
                    let sum = sum.clone();
                    thread_pool.add_task(move || {
                        let mut sum = sum.lock().unwrap();
                        *sum += i;
                    });
                }
                assert_eq!(thread_pool.worker_count(), 5);
            }
            let sum = sum.lock().unwrap();
            assert_eq!(*sum, 5050);
        }
    }
}
