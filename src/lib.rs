#![warn(missing_docs)]

//! Provides the ability to perform external sorts on structs

mod external_sort;

pub use crate::external_sort::{ExtSortedIterator, ExternalSorter, ExternallySortable};
