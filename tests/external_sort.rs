extern crate external_sort;
#[macro_use]
extern crate serde_derive;

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
    let iter = ExternalSorter::new(2, None).sort(unsorted.into_iter());
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.the_num, sorted[idx].the_num);
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
    let iter = ExternalSorter::new(16, None).sort_by(unsorted.into_iter(), |a, b| b.cmp(a));
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.the_num, sorted[idx].the_num);
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
    let iter = ExternalSorter::new(0, None).sort(unsorted.into_iter());
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.the_num, sorted[idx].the_num);
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
    let iter = ExternalSorter::new(999999999, None).sort(unsorted.into_iter());
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.the_num, sorted[idx].the_num);
    }
}
