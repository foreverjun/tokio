use crate::loom::sync::Arc;
use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
use crate::runtime::scheduler::multi_thread::Handle;
use crate::runtime::task;
use faa_array_queue::FAAArrayQueue;

pub(crate) struct FAAArray {
    queue: FAAArrayQueue<task::Header>,
}

impl FAAArray {
    pub(crate) fn new(inject_min: usize, transfer_size: usize) -> QueueHolder<FAAArray> {
        QueueHolder::new(
            Self {
                queue: FAAArrayQueue::<task::Header>::new(),
            },
            inject_min,
            transfer_size,
        )
    }
}
pub(crate) struct FAAArrayIter<'a> {
    queue: &'a FAAArray,
    remaining: usize,
}

impl Iterator for FAAArrayIter<'_> {
    type Item = task::Notified<Arc<Handle>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.queue.pop()
    }
}

impl FastQueue for FAAArray {
    type Iter<'a> = FAAArrayIter<'a>;
    fn push(&self, task: task::RawTask) {
        self.queue.enqueue(task.as_mut_ptr());
    }

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = task::RawTask>,
    {
        for t in tasks {
            self.queue.enqueue(t.as_mut_ptr());
        }
    }

    fn pop(&self) -> Option<task::Notified<Arc<Handle>>> {
        let ptr = self.queue.dequeue();
        if ptr.is_null() {
            return None;
        }
        unsafe { Some(task::Notified::from_raw(task::RawTask::from_mut_ptr(ptr))) }
    }

    fn pop_n(&self, n: usize) -> FAAArrayIter<'_> {
        FAAArrayIter {
            queue: self,
            remaining: n,
        }
    }
}
