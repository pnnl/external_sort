external_sort
=============

Provides the ability to perform external sorts on structs, which allows for rapid sorting of extremely large data streams.

Usage
-----

Add this to your `Cargo.toml`:

```
[dependencies]
external_sort = "^0.0.1"
```

and this to your crate root:

```
extern crate external_sort;
```

Examples
--------

The following shows using `external_sort` to sort a vector of simple structs.

Note that your struct must `impl` `Ord`, `Clone`, as well as the `serde` `Serialize` and `Deserialize` traits. Additionally, in order for `external_sort` to track it's memory buffer usage, your struct must be able to report on it's size (via `external_sort::ExternallySortable`)

```
extern crate external_sort;
#[macro_use]
extern crate serde_derive;

use external_sort::{ExternallySortable, ExternalSorter};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Num {
    the_num: u8
}

impl Num {
    fn new(num:u8) -> Num {
        Num { the_num: num }
    }
}

impl ExternallySortable for Num {
    fn get_size(&self) -> u64 {
        8
    }
}

fn main() {
    let unsorted = vec![Num::new(5), Num::new(2), Num::new(1), Num::new(3), Num::new(4)];
    let sorted = vec![Num::new(1), Num::new(2), Num::new(3), Num::new(4), Num::new(5)];

    let external_sorter = ExternalSorter::new(16, None);
    let iter = external_sorter.sort(unsorted.into_iter());
    for (idx, i) in iter.enumerate() {
        assert_eq!(i.the_num, sorted[idx].the_num);
    }
}
```