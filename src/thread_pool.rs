use crossbeam::channel::{bounded, Sender};
use std::thread;

use crate::types::Result;

/// The ratio of the task buffer size to the number of worker threads.
const TASK_WORKER_RATIO: usize = 2;

pub struct ThreadPool {
    workers: Vec<thread::JoinHandle<()>>,
    sender: Option<Sender<Box<dyn FnOnce() + Send + 'static>>>,
}

impl ThreadPool {
    pub fn new(num: usize) -> Self {
        let (sender, receiver) =
            bounded::<Box<dyn FnOnce() + Send + 'static>>(num * TASK_WORKER_RATIO);
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
            worker.join().expect("Unable to join a worker thread");
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
            let sum = std::sync::Arc::new(std::sync::Mutex::new(0));
            {
                let thread_pool = ThreadPool::new(5);
                thread::sleep(std::time::Duration::from_micros(10u64)); // Make workers wait.
                for i in 1..=100 {
                    let sum = sum.clone();
                    thread_pool
                        .add_task(move || {
                            let mut sum = sum.lock().unwrap();
                            *sum += i;
                        })
                        .expect(&format!("Failed to add_task for {}", i));
                }
                assert_eq!(thread_pool.worker_count(), 5);
            }
            let sum = sum.lock().unwrap();
            assert_eq!(*sum, 5050);
        }
    }
}
