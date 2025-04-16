use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
use crate::runtime::task::{Notified, Schedule};
use crossbeam_queue::SegQueue;

pub(crate) struct Crossbeam<T: Schedule> {
    queue: SegQueue<Notified<T>>,
}

impl<T: Schedule> Crossbeam<T> {
    pub(crate) fn new(inject_min: usize, transfer_size: usize) -> QueueHolder<T, Crossbeam<T>> {
        QueueHolder::new(
            Self {
                queue: SegQueue::<Notified<T>>::default(),
            },
            inject_min,
            transfer_size,
        )
    }
}
pub(crate) struct CrossbeamIter<'a, T: Schedule> {
    queue: &'a Crossbeam<T>,
    remaining: usize,
}

impl<'a, T: 'static + Schedule> Iterator for CrossbeamIter<'a, T> {
    type Item = Notified<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.queue.pop()
    }
}

impl<T: 'static + Schedule> FastQueue<T> for Crossbeam<T> {
    type Iter<'a> = CrossbeamIter<'a, T>;
    fn push(&self, task: Notified<T>) {
        self.queue.push(task);
    }

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = Notified<T>>,
    {
        for t in tasks {
            self.queue.push(t);
        }
    }

    fn pop(&self) -> Option<Notified<T>> {
        self.queue.pop()
    }

    fn pop_n(&self, n: usize) -> CrossbeamIter<'_, T> {
        CrossbeamIter {
            queue: self,
            remaining: n,
        }
    }
}
