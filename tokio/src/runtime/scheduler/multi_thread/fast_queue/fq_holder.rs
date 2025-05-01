use super::FastQueue;

pub(crate) struct QueueHolder<Q: FastQueue> {
    queue: Q,
    inject_min: usize,    // The inject size at which tasks can be transferred
    transfer_size: usize, // Number of tasks to be moved to the queue from inject
}
// Transfer from inject occurs when len >= inject_min + transfer_size

impl<Q: FastQueue> QueueHolder<Q> {
    pub(crate) fn new(queue: Q, inject_min: usize, transfer_size: usize) -> Self {
        Self {
            queue,
            inject_min,
            transfer_size,
        }
    }
    pub(crate) fn queue(&self) -> &Q {
        &self.queue
    }

    pub(crate) fn inject_min(&self) -> usize {
        self.inject_min
    }

    pub(crate) fn transfer_size(&self) -> usize {
        self.transfer_size
    }
}
