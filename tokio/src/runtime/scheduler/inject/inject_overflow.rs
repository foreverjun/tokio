use crate::runtime::scheduler::inject::{Shared, Synced};
#[cfg(feature = "rt-multi-thread")]
use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
#[cfg(feature = "rt-multi-thread")]
use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
#[cfg(feature = "rt-multi-thread")]
use crate::runtime::scheduler::Lock;
use crate::runtime::task;
use std::sync::atomic::Ordering::Release;

impl<T: 'static> Shared<T> {
    /// Pushes several values into the queue.
    ///
    /// # Safety
    ///
    /// Must be called with the same `Synced` instance returned by `Inject::new`
    #[inline]
    pub(crate) unsafe fn push_batch_overflow<L, I, Q>(
        &self,
        shared: L,
        mut iter: I,
        queue_holder: &QueueHolder<T, Q>,
    ) where
        L: Lock<Synced>,
        I: Iterator<Item = task::Notified<T>>,
        Q: FastQueue<T>,
    {
        let first = match iter.next() {
            Some(first) => first.into_raw(),
            None => return,
        };

        // Link up all the tasks.
        let mut prev = first;
        let mut counter = 1;

        // We are going to be called with an `std::iter::Chain`, and that
        // iterator overrides `for_each` to something that is easier for the
        // compiler to optimize than a loop.
        iter.for_each(|next| {
            let next = next.into_raw();

            // safety: Holding the Notified for a task guarantees exclusive
            // access to the `queue_next` field.
            unsafe { prev.set_queue_next(Some(next)) };
            prev = next;
            counter += 1;
        });

        // Now that the tasks are linked together, insert them into the
        // linked list.
        self.push_batch_inner_overflow(shared, first, prev, counter, queue_holder);
    }

    /// Inserts several tasks that have been linked together into the queue.
    ///
    /// The provided head and tail may be be the same task. In this case, a
    /// single task is inserted.
    #[inline]
    unsafe fn push_batch_inner_overflow<L, Q>(
        &self,
        shared: L,
        batch_head: task::RawTask,
        batch_tail: task::RawTask,
        num: usize,
        queue_holder: &QueueHolder<T, Q>,
    ) where
        L: Lock<Synced>,
        Q: FastQueue<T>,
    {
        debug_assert!(unsafe { batch_tail.get_queue_next().is_none() });

        let mut synced_lock = shared.lock();

        if synced_lock.as_mut().is_closed {
            drop(synced_lock);

            let mut curr = Some(batch_head);

            while let Some(task) = curr {
                curr = task.get_queue_next();

                let _ = unsafe { task::Notified::<T>::from_raw(task) };
            }

            return;
        }

        let synced_mut = synced_lock.as_mut();

        if let Some(tail) = synced_mut.tail {
            unsafe {
                tail.set_queue_next(Some(batch_head));
            }
        } else {
            synced_mut.head = Some(batch_head);
        }

        synced_mut.tail = Some(batch_tail);

        // safety: All updates to the len atomic are guarded by the mutex. As
        // such, a non-atomic load followed by a store is safe.
        let current_len = self.len.unsync_load();
        let transfer_size = queue_holder.transfer_size();
        let transfer_border = queue_holder.inject_min() + transfer_size;

        let new_len = current_len + num;

        if new_len > transfer_border {
            let mut tasks_to_transfer = Vec::with_capacity(transfer_size);

            for _ in 0..transfer_size {
                match synced_mut.pop() {
                    Some(task) => tasks_to_transfer.push(task),
                    None => break,
                }
            }

            let transferred = tasks_to_transfer.len();
            self.len.store(new_len - transferred, Release);
            drop(synced_lock);
            queue_holder
                .queue()
                .push_batch(tasks_to_transfer.into_iter());
        } else {
            self.len.store(new_len, Release);
        }
    }
}
