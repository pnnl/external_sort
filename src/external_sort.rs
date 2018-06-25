use std::clone::Clone;
use std::cmp::Ordering::{self, Less};
use std::collections::VecDeque;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::SeekFrom::Start;
use std::io::{BufRead, BufReader, Seek, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use tempdir::TempDir;

/// Trait for types that can be used by
/// [ExternalSorter](struct.ExternalSorter.html). Must be sortable, cloneable,
/// serializeable, and able to report on it's size
pub trait ExternallySortable: Ord + Clone + Serialize + DeserializeOwned {
    /// Get the size, in bytes, of this object (used to constrain the buffer
    /// used in the external sort).
    ///
    /// If you are unable to calculate a size, simply return `1` from this
    /// function, and then set the `buffer_bytes` to the number of objects
    /// to hold in memory when creating an
    /// [ExternalSorter](struct.ExternalSorter.html)
    fn get_size(&self) -> u64;
}

/// Iterator that provides sorted `T`s
pub struct ExtSortedIterator<T> {
    buffers: Vec<VecDeque<T>>,
    chunk_offsets: Vec<u64>,
    max_per_chunk: u64,
    chunks: u64,
    tmp_dir: TempDir,
    sort_by_fn: Box<FnMut(&T, &T) -> Ordering>,
    failed: bool,
}

impl<T> Iterator for ExtSortedIterator<T>
where
    T: ExternallySortable,
{
    type Item = Result<T, Box<Error>>;

    ///
    /// # Errors
    ///
    /// This method can fail due to issues reading intermediate sorted chunks
    /// from disk, or due to serde deserialization issues
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        // fill up any empty buffers
        let mut empty = true;
        for chunk_num in 0..self.chunks {
            if self.buffers[chunk_num as usize].is_empty() {
                let mut f = match File::open(self.tmp_dir.path().join(chunk_num.to_string())) {
                    Ok(f) => f,
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(Box::new(e)));
                    }
                };
                match f.seek(Start(self.chunk_offsets[chunk_num as usize])) {
                    Ok(_) => (),
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(Box::new(e)));
                    }
                }
                let bytes_read =
                    match fill_buff(&mut self.buffers[chunk_num as usize], f, self.max_per_chunk) {
                        Ok(bytes_read) => bytes_read,
                        Err(e) => {
                            self.failed = true;
                            return Some(Err(e));
                        }
                    };
                self.chunk_offsets[chunk_num as usize] += bytes_read;
                if !self.buffers[chunk_num as usize].is_empty() {
                    empty = false;
                }
            } else {
                empty = false;
            }
        }
        if empty {
            return None;
        }

        // find the next record to write
        // check is_empty() before unwrap()ing
        let mut idx = 0;
        for chunk_num in 0..self.chunks as usize {
            if !self.buffers[chunk_num].is_empty() {
                if self.buffers[idx].is_empty()
                    || (self.sort_by_fn)(
                        self.buffers[chunk_num].front().unwrap(),
                        self.buffers[idx].front().unwrap(),
                    ) == Less
                {
                    idx = chunk_num;
                }
            }
        }

        // unwrap due to checks above
        let r = self.buffers[idx].pop_front().unwrap();
        Some(Ok(r))
    }
}

/// Perform an external sort on an unsorted stream of incoming data
///
/// # Examples
///
/// ```
/// extern crate external_sort;
/// #[macro_use]
/// extern crate serde_derive;
///
/// use external_sort::{ExternallySortable, ExternalSorter};
///
/// #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// struct Num {
///     the_num: u32
/// }
///
/// impl Num {
///     fn new(num:u32) -> Num {
///         Num { the_num: num }
///     }
/// }
///
/// impl ExternallySortable for Num {
///     fn get_size(&self) -> u64 {
///         4
///     }
/// }
///
/// fn main() {
///     let unsorted = vec![Num::new(5), Num::new(2), Num::new(1), Num::new(3),
///         Num::new(4)]; 
///     let sorted = vec![Num::new(1), Num::new(2), Num::new(3), Num::new(4), 
///         Num::new(5)];
///
///     let external_sorter = ExternalSorter::new(16, None);
///     let iter = external_sorter.sort(unsorted.into_iter()).unwrap();
///     for (idx, i) in iter.enumerate() {
///         assert_eq!(i.unwrap().the_num, sorted[idx].the_num);
///     }
/// }
/// ```
pub struct ExternalSorter<T>
where
    T: ExternallySortable,
{
    tmp_dir: Option<PathBuf>,
    buffer_bytes: u64,
    phantom: PhantomData<T>,
}

impl<T> ExternalSorter<T>
where
    T: ExternallySortable,
{
    /// Create a new `ExternalSorter` with a specified memory buffer and
    /// temporary directory
    pub fn new(buffer_bytes: u64, tmp_dir: Option<PathBuf>) -> ExternalSorter<T> {
        ExternalSorter {
            buffer_bytes,
            tmp_dir,
            phantom: PhantomData,
        }
    }

    /// Sort the `T`s provided by `unsorted` and return a sorted (ascending)
    /// iterator
    ///
    /// # Errors
    ///
    /// This method can fail due to issues writing intermediate sorted chunks
    /// to disk, or due to serde serialization issues
    pub fn sort<I>(&self, unsorted: I) -> Result<ExtSortedIterator<T>, Box<Error>>
    where
        I: Iterator<Item = T>,
    {
        self.sort_by(unsorted, |a, b| a.cmp(b))
    }

    /// Sort (based on `compare`) the `T`s provided by `unsorted` and return an
    /// iterator
    ///
    /// # Errors
    ///
    /// This method can fail due to issues writing intermediate sorted chunks
    /// to disk, or due to serde serialization issues
    pub fn sort_by<I, F>(&self, unsorted: I, compare: F) -> Result<ExtSortedIterator<T>, Box<Error>>
    where
        I: Iterator<Item = T>,
        F: 'static + FnMut(&T, &T) -> Ordering,
    {
        let tmp_dir = match self.tmp_dir {
            Some(ref p) => TempDir::new_in(p, "sort_fasta")?,
            None => TempDir::new("sort_fasta")?,
        };
        // creating the thing we need to return first due to the face that we need to
        // borrow tmp_dir and move it out
        let mut iter = ExtSortedIterator {
            buffers: Vec::new(),
            chunk_offsets: Vec::new(),
            max_per_chunk: 0,
            chunks: 0,
            tmp_dir,
            sort_by_fn: Box::new(compare),
            failed: false,
        };

        {
            let mut total_read = 0;
            let mut chunk = Vec::new();

            // make the initial chunks on disk
            for seq in unsorted {
                total_read += seq.get_size();
                chunk.push(seq);
                if total_read >= self.buffer_bytes {
                    chunk.sort_by(|a, b| (iter.sort_by_fn)(a, b));
                    self.write_chunk(
                        &iter.tmp_dir.path().join(iter.chunks.to_string()),
                        &mut chunk,
                    )?;
                    chunk.clear();
                    total_read = 0;
                    iter.chunks += 1;
                }
            }
            // write the last chunk
            if chunk.len() > 0 {
                chunk.sort_by(|a, b| (iter.sort_by_fn)(a, b));
                self.write_chunk(
                    &iter.tmp_dir.path().join(iter.chunks.to_string()),
                    &mut chunk,
                )?;
                iter.chunks += 1;
            }

            // initialize buffers for each chunk
            iter.max_per_chunk = self.buffer_bytes / iter.chunks;
            iter.buffers = vec![VecDeque::new(); iter.chunks as usize];
            iter.chunk_offsets = vec![0 as u64; iter.chunks as usize];
            for chunk_num in 0..iter.chunks {
                let offset = fill_buff(
                    &mut iter.buffers[chunk_num as usize],
                    File::open(iter.tmp_dir.path().join(chunk_num.to_string()))?,
                    iter.max_per_chunk,
                )?;
                iter.chunk_offsets[chunk_num as usize] = offset;
            }
        }

        Ok(iter)
    }

    fn write_chunk(&self, file: &PathBuf, chunk: &mut Vec<T>) -> Result<(), Box<Error>> {
        let mut new_file = OpenOptions::new().create(true).append(true).open(file)?;
        for s in chunk {
            let mut serialized = serde_json::to_string(&s)?;
            serialized.push_str("\n");
            new_file.write_all(serialized.as_bytes())?;
        }

        Ok(())
    }
}

fn fill_buff<T>(vec: &mut VecDeque<T>, file: File, max_bytes: u64) -> Result<u64, Box<Error>>
where
    T: ExternallySortable,
{
    let mut total_read = 0;
    let mut bytes_read = 0;
    for line in BufReader::new(file).lines() {
        let line_s = line?;
        bytes_read += line_s.len() + 1;
        let deserialized: T = serde_json::from_str(&line_s)?;
        total_read += deserialized.get_size();
        vec.push_back(deserialized);
        if total_read > max_bytes {
            break;
        }
    }

    Ok(bytes_read as u64)
}
