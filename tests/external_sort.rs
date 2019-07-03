use rand;
use serde::{Deserialize, Serialize};

use std::env;
use std::fs;

use external_sort::{ExternalSorter, ExternallySortable};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Num {
    the_num: u8,
}

impl Num {
    fn new(num: u8) -> Num {
        Num { the_num: num }
    }
}

impl ExternallySortable for Num {
    fn get_size(&self) -> u64 {
        1
    }
}

#[test]
fn sort() {
    let unsorted = vec![
        Num::new(5),
        Num::new(2),
        Num::new(1),
        Num::new(3),
        Num::new(4),
    ];
    let sorted = vec![
        Num::new(1),
        Num::new(2),
        Num::new(3),
        Num::new(4),
        Num::new(5),
    ];
    let iter = ExternalSorter::new(2, None)
        .sort(unsorted.into_iter())
        .unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}

#[test]
fn reverse() {
    let unsorted = vec![
        Num::new(5),
        Num::new(2),
        Num::new(1),
        Num::new(3),
        Num::new(4),
    ];
    let sorted = vec![
        Num::new(5),
        Num::new(4),
        Num::new(3),
        Num::new(2),
        Num::new(1),
    ];
    let iter = ExternalSorter::new(16, None)
        .sort_by(unsorted.into_iter(), |a, b| b.cmp(a))
        .unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}

#[test]
fn zero_buff() {
    let unsorted = vec![
        Num::new(5),
        Num::new(2),
        Num::new(1),
        Num::new(3),
        Num::new(4),
    ];
    let sorted = vec![
        Num::new(1),
        Num::new(2),
        Num::new(3),
        Num::new(4),
        Num::new(5),
    ];
    let iter = ExternalSorter::new(0, None)
        .sort(unsorted.into_iter())
        .unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}

#[test]
fn large_buff() {
    let unsorted = vec![
        Num::new(5),
        Num::new(2),
        Num::new(1),
        Num::new(3),
        Num::new(4),
    ];
    let sorted = vec![
        Num::new(1),
        Num::new(2),
        Num::new(3),
        Num::new(4),
        Num::new(5),
    ];
    let iter = ExternalSorter::new(999999999, None)
        .sort(unsorted.into_iter())
        .unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}

#[test]
fn reuse() {
    let unsorted = vec![
        Num::new(5),
        Num::new(2),
        Num::new(1),
        Num::new(3),
        Num::new(4),
    ];
    let sorted = vec![
        Num::new(1),
        Num::new(2),
        Num::new(3),
        Num::new(4),
        Num::new(5),
    ];
    let iter = ExternalSorter::new(2, None)
        .sort(unsorted.clone().into_iter())
        .unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
    let iter2 = ExternalSorter::new(2, None)
        .sort(unsorted.into_iter())
        .unwrap();
    for (idx, i) in iter2.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}

#[test]
fn large() {
    let mut unsorted = Vec::new();
    for _ in 0..10_000 {
        unsorted.push(Num::new(rand::random()));
    }
    let iter = ExternalSorter::new(100, None)
        .sort(unsorted.into_iter())
        .unwrap();
    let mut last = 0;
    for i in iter {
        let n = i.unwrap().the_num;
        assert!(n >= last);
        last = n;
    }
}

#[test]
fn handle_fail() {
    let mut unsorted = Vec::new();
    for _ in 0..10_000 {
        unsorted.push(Num::new(rand::random()));
    }
    let r = env::temp_dir().join("external_sort_test");
    fs::create_dir_all(r.clone()).unwrap();

    let iter = ExternalSorter::new(100, Some(r.clone()))
        .sort(unsorted.into_iter())
        .unwrap();
    fs::remove_dir_all(r.clone()).unwrap();
    let mut fail = false;
    for i in iter {
        if i.is_err() {
            fail = true;
        }
    }
    assert!(fail);
}
