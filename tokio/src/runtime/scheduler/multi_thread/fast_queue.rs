use crate::runtime::task;

use crate::loom::sync::Arc;
use crate::runtime::scheduler::multi_thread::Handle;

pub(crate) mod faaarray;
pub(crate) mod fq_holder;
pub(crate) trait FastQueue {
    type Iter<'a>: Iterator<Item = task::Notified<Arc<Handle>>>
    where
        Self: 'a;
    #[allow(dead_code)]
    fn push(&self, task: task::RawTask);

    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = task::RawTask>;

    fn pop(&self) -> Option<task::Notified<Arc<Handle>>>;

    fn pop_n(&self, n: usize) -> Self::Iter<'_>;
}
