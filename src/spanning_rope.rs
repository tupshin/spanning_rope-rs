#![feature(question_mark)]
#![feature(btree_range, collections_bound)]
#[macro_use]
extern crate chan;
extern crate uuid;

use std::collections::Bound::{Included, Unbounded};

use uuid::Uuid;
// use std::sync::mpsc::sync_channel;
use std::rc::Rc;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::fmt::Debug;
// use std::thread;
// use std::sync::mpsc::SyncSender;

const MAX_SEGMENT_SIZE: usize = 10;

///A Spanning Rope is a rope-like construct that spans the total ordering of an entire namespace,
///or set of nested namespaces
///It is notably influenced by both the concepts of
///[Ropes](https://en.wikipedia.org/wiki/Rope_(data_structure))
///as well as [Spanning Trees](https://en.wikipedia.org/wiki/Spanning_tree).
///It is designed to handle nested
///ordered namespaces without
///a priori knowledge of what those namespaces will be.
pub struct SpanningRope<K, V> {
    #[allow(dead_code)]
    parent: Option<Rc<SpanningRope<K, V>>>,
    interior: Interior<K, V>,
    range: Range<K>,
    segment_id: Uuid,
}


impl<K, V> SpanningRope<K, V>
    where K: PartialOrd + Clone + Copy + Ord + Debug + Hash,
          V: Clone + Debug
{
    pub fn owns(&self, k: K) -> bool {
        self.range.contains(k)
    }

    pub fn new(start: Option<K>, end: Option<K>) -> SpanningRope<K, V> {
        SpanningRope {
            parent: None,
            interior: Interior::Local(LocalStorage {
                kv: BTreeMap::new(),
                storage_id: Uuid::new_v4(),
            }),
            range: Range {
                start: start,
                end: end, // local_storage: HashMap::new(),
            },
            segment_id: Uuid::new_v4(),
        }
    }

    pub fn get(&self, k: K) -> Result<Option<V>, StorageError> {
        match self.interior {
            Interior::Segments(ref segments) => {
                for segment in segments.into_iter() {
                    if segment.owns(k) {
                        return segment.get(k);
                    }
                }
                Err(StorageError::OutOfRange)

            }
            Interior::Local(ref local_storage) => {
                match local_storage.kv.get(&k) {
                    Some(v) => Ok(Some(v.clone())),
                    None => Ok(None),
                }
            }
        }
    }

    fn should_split_at(&self) -> Option<K> {
        if self.key_count() > MAX_SEGMENT_SIZE {
            match self.interior {
                Interior::Local(ref local) => {
                    match local.kv.iter().nth(MAX_SEGMENT_SIZE/2) {
                        Some(item) => Some(item.0.clone()),
                        None => None
                    }
                } 
                Interior::Segments(ref segments) => {
                    for segment in segments.into_iter() {
                    	return segment.should_split_at()
                    }
                    None
                }
                //self.
            }
        } else {
            None
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Result<(), StorageError> {
        match self.should_split_at() {
            Some(split_token) => self.split_at(split_token),
            None => {},
        };
        match self.interior {
            Interior::Local(ref mut local) => {
                println!("inserting {:?}:{:?} into {:?}", k, v, self.segment_id);
                local.kv.insert(k, v);
                return Ok(())
            }
            Interior::Segments(ref mut segments) => {
                for mut segment in segments.into_iter() {
                    if segment.owns(k) {
                        println!("inserting {:?}:{:?} into {:?}", k, v, self.segment_id);
                        return segment.insert(k, v);
                    }
                }
            }
        }
        Err(StorageError::OutOfRange)
    }
}

pub trait StatsReporter {
    fn key_count(&self) -> usize;
    fn internal_segment_count(&self) -> usize;
}

trait Splittable<K> {
    fn split_at(&mut self, k: K);
}

impl<K, V> Splittable<K> for SpanningRope<K, V>
    where K: Ord + Hash + Copy + Clone + Debug,
          V: Debug + Clone
{
    fn split_at(&mut self, k: K) {
        let interior = match self.interior {
            Interior::Local(ref local) => {
                let range = match (self.range.start.clone(), self.range.end.clone()) {
                    (None, None) => {
                        (local.kv.range(Unbounded, Included(&k)),
                         local.kv.range(Included(&k), Unbounded))
                    }
                    (Some(left), None) => {
                        (local.kv.range(Included(&left), Included(&k)),
                         local.kv.range(Included(&k), Unbounded))
                    }
                    (None, Some(right)) => {
                        (local.kv.range(Unbounded, Included(&k)),
                         local.kv.range(Included(&k), Included(&right)))
                    }
                    (Some(left), Some(right)) => {
                        (local.kv.range(Included(&left), Included(&k)),
                         local.kv.range(Included(&k), Included(&right)))
                    }
                };

                let mut left = SpanningRope::new(None, Some(k));
                let mut right = SpanningRope::new(Some(k), None);

                for item in range.0.enumerate() {
                    left.insert((item.1).0.clone(), (item.1).1.clone()).unwrap();
                }
                for item in range.1.enumerate() {
                    right.insert((item.1).0.clone(), (item.1).1.clone()).unwrap();
                }
                let mut interior = vec![];
                interior.push(left);
                interior.push(right);
                Some(Interior::Segments(interior))
            }
            Interior::Segments(ref mut segments) => {
                     for mut segment in segments.iter_mut() {
                    	match segment.should_split_at() {
                    	    Some(token) => segment.split_at(token),
                    	    None => {}
                    	}
                    }
                     None
            }
        };
        self.interior = interior.unwrap();
    }
}


impl<K, V> StatsReporter for SpanningRope<K, V> {
    fn key_count(&self) -> usize {
        let mut count = 0usize;
        match self.interior {
            Interior::Local(ref local) => local.kv.keys().count(),
            Interior::Segments(ref segments) => {
                for segment in segments.into_iter() {
                    count += segment.key_count();
                }
                count
            }
        }
    }

    fn internal_segment_count(&self) -> usize {
        match self.interior {
            Interior::Local(_) => 0,
            Interior::Segments(ref segments) => segments.len(),
        }
    }
}

#[derive(Debug)]
pub struct LocalStorage<K, V> {
    kv: BTreeMap<K, V>,
    storage_id: Uuid,
}

pub enum Interior<K, V> {
    Segments(Vec<SpanningRope<K, V>>),
    Local(LocalStorage<K, V>),
}


#[derive(Debug)]
pub enum StorageError {
    OutOfRange,
}


///Data about the local range. Responsible for the persisting and retrieval of its own ks
#[derive(Clone,Copy)]
struct Range<K> {
    start: Option<K>,
    end: Option<K>,
}

impl<K> Range<K>
    where K: Copy + Clone + PartialOrd
{
    pub fn contains(&self, k: K) -> bool {
        match (self.start.clone(), self.end.clone()) {
            // If this Rope has no upper or lower bounds, than it is
            // authoritative for the entire range
            (None, None) => true,
            (Some(start), None) => k >= start,
            (None, Some(end)) => k <= end,
            (Some(start), Some(end)) => k >= start && k <= end,
        }
    }
}
