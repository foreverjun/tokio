#[cfg(feature = "rt-multi-thread")]
mod multi_thread_push_overflow {
    use crate::runtime::scheduler::inject::{Shared, Synced};
    use crate::runtime::scheduler::multi_thread::fast_queue::fq_holder::QueueHolder;
    use crate::runtime::scheduler::multi_thread::fast_queue::FastQueue;
    use crate::runtime::scheduler::Lock;
    use crate::runtime::task;
    use std::sync::atomic::Ordering::Release;
    use std::mem::MaybeUninit;

    const TRANSFER_SIZE: usize = 256;

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
            queue_holder: &QueueHolder<Q>,
        ) where
            L: Lock<Synced>,
            I: Iterator<Item = task::Notified<T>>,
            Q: FastQueue,
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
            queue_holder: &QueueHolder<Q>,
        ) where
            L: Lock<Synced>,
            Q: FastQueue,
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
            // let transfer_size = queue_holder.transfer_size();
            let transfer_border = queue_holder.inject_min() + TRANSFER_SIZE;

            let new_len = current_len + num;

            if new_len > transfer_border {
                let mut tasks_to_transfer: [MaybeUninit<task::RawTask>; TRANSFER_SIZE] =
                [MaybeUninit::uninit(); TRANSFER_SIZE];
                let mut transferred = 0;

                for i in 0..TRANSFER_SIZE {
                    if let Some(task) = synced_mut.head {
                        synced_mut.head = unsafe { task.get_queue_next() };

                        if synced_mut.head.is_none() {
                            synced_mut.tail = None;
                        }

                        unsafe { task.set_queue_next(None) };
                        tasks_to_transfer[i].write(task);
                        transferred += 1;
                    } else {
                        break;
                    }
                }
                self.len.store(new_len - transferred, Release);
                drop(synced_lock);
                for i in 0..transferred {
                    queue_holder
                        .queue()
                        .push(unsafe { tasks_to_transfer[i].assume_init() });
                }
            } else {
                self.len.store(new_len, Release);
            }
        }
    }
}
