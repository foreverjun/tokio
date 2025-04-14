use crate::runtime::task;

pub(crate) mod fq_holder;
pub(crate) mod crossbeam;

pub(crate) trait FastQueue<T: 'static>: Send + Sync {
    type Iter<'a>: Iterator<Item = task::Notified<T>>
    where
        Self: 'a;
    
    fn push(&self, task: task::Notified<T>);
    
    fn push_batch<I>(&self, tasks: I)
    where
        I: Iterator<Item = task::Notified<T>>;
    
    fn pop(&self) -> Option<task::Notified<T>>;
    
    fn pop_n<'a>(&'a self, n: usize) -> Self::Iter<'a>;
}