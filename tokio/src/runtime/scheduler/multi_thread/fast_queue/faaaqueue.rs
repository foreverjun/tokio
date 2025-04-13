use faa_array_queue::FaaArrayQueue;
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
use crate::runtime::task::{Notified, Schedule};

pub(crate) struct FAAAQueue<T : Schedule>{
    queue : FaaArrayQueue<Notified<T>>
}


impl <T: Schedule> FAAAQueue<T>{
    pub(crate) fn new(inject_min: usize, transfer_size: usize) -> QueueHolder<T, FAAAQueue<T>>{
        QueueHolder::new(Self{ queue: FaaArrayQueue::<Notified<T>>::default()}, inject_min, transfer_size)
    }
}
pub(crate) struct FAAAQueueIter<'a, T : Schedule> {
    queue: &'a FAAAQueue<T>,
    remaining: usize,
}

impl<'a, T: 'static + Schedule> Iterator for FAAAQueueIter<'a, T> {
    type Item = Notified<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.queue.pop()
    }
}

impl <T: 'static + Schedule> FastQueue<T> for FAAAQueue<T> {
    type Iter<'a> = FAAAQueueIter<'a, T>;
    fn push(&self, task: Notified<T>) {
        self.queue.enqueue(task);
    }

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item=Notified<T>>
    {
        for t in tasks {
            self.queue.enqueue(t);
        }
    }

    fn pop(&self) -> Option<Notified<T>> {
        self.queue.dequeue()
    }

    fn pop_n(&self, n: usize) -> FAAAQueueIter<'_,T> {
        FAAAQueueIter {
            queue: self,
            remaining: n,
        }
    }
}