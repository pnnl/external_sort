use serde::{Serialize, Deserialize};
use external_sort::{ExternalSorter, ExternallySortable};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Num {
    the_num: u32,
}

impl Num {
    fn new(num: u32) -> Num {
        Num { the_num: num }
    }
}

impl ExternallySortable for Num {
    fn get_size(&self) -> u64 {
        4
    }
}

fn main() {
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

    let external_sorter = ExternalSorter::new(16, None);
    let iter = external_sorter.sort(unsorted.into_iter()).unwrap();
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
    }
}
