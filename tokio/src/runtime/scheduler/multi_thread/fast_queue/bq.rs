use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
use crate::runtime::task::{Notified, Schedule};
use batching_queue::BQueue;
use batching_queue::DeqBatchIterator;

pub(crate) struct BqQueue<T: Schedule> {
    queue: BQueue<Notified<T>>,
}

impl<T: Schedule> BqQueue<T> {
    pub(crate) fn new(inject_min: usize, transfer_size: usize)
        -> QueueHolder<T, BqQueue<T>>
    {
        QueueHolder::new(
            BqQueue {
                queue: BQueue::<Notified<T>>::new(),
            },
            inject_min,
            transfer_size,
        )
    }
}

pub(crate) struct BqIter<T: Schedule> {
    inner: DeqBatchIterator<Notified<T>>,
}

impl<'a, T: Schedule> Iterator for BqIter<T> {
    type Item = Notified<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<T: 'static + Schedule> FastQueue<T> for BqQueue<T> {
    type Iter<'a> = BqIter<T>;

    fn push(&self, task: Notified<T>) {
        self.queue.enqueue(task);
    }

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = Notified<T>>,
    {
        self.queue.enqueue_batch(tasks);
    }

    fn pop(&self) -> Option<Notified<T>> {
        self.queue.dequeue()
    }

    fn pop_n(&self, n: usize) -> Self::Iter<'_> {
        BqIter {
            inner: self.queue.deq_batch(n),
        }
    }
}

