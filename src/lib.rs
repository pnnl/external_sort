#![warn(missing_docs)]

//! Provides the ability to perform external sorts on structs

extern crate serde;
extern crate serde_json;
extern crate tempdir;

mod external_sort;

pub use external_sort::{ExtSortedIterator, ExternalSorter, ExternallySortable};
