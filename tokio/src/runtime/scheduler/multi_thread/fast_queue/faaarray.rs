use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
use crate::runtime::task::{Notified, Schedule};
use faa_array_queue::FAAArrayQueue;

pub(crate) struct FAAArray<T: Schedule> {
    queue: FAAArrayQueue<Notified<T>>,
}

impl<T: Schedule> FAAArray<T> {
    pub(crate) fn new(inject_min: usize, transfer_size: usize) -> QueueHolder<T, FAAArray<T>> {
        QueueHolder::new(
            Self {
                queue: FAAArrayQueue::<Notified<T>>::new(),
            },
            inject_min,
            transfer_size,
        )
    }
}
pub(crate) struct FAAArrayIter<'a, T: Schedule> {
    queue: &'a FAAArray<T>,
    remaining: usize,
}

impl<'a, T: 'static + Schedule> Iterator for FAAArrayIter<'a, T> {
    type Item = Notified<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.queue.pop()
    }
}

impl<T: 'static + Schedule> FastQueue<T> for FAAArray<T> {
    type Iter<'a> = FAAArrayIter<'a, T>;
    fn push(&self, task: Notified<T>) {
        self.queue.enqueue(task);
    }

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = Notified<T>>,
    {
        for t in tasks {
            self.queue.enqueue(t);
        }
    }

    fn pop(&self) -> Option<Notified<T>> {
        self.queue.dequeue()
    }

    fn pop_n(&self, n: usize) -> FAAArrayIter<'_, T> {
        FAAArrayIter {
            queue: self,
            remaining: n,
        }
    }
}
