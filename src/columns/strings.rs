use value::ValueType;
use columns::{ColumnData, ColIter, UniqueValues};
use heapsize::HeapSizeOf;
use std::collections::hash_set::HashSet;
use std::collections::HashMap;
use std::rc::Rc;
use std::str;
use std::{u8, u16, usize};
use std::iter::FromIterator;
use std::slice;
use std::iter;
use num::NumCast;
use std::ops::Deref;
use std::marker::PhantomData;

pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn build_string_column(values: Vec<Option<Rc<String>>>, unique_values: UniqueValues<Option<Rc<String>>>) -> Box<ColumnData> {
    if let Some(u) = unique_values.get_values() {
        // Box::new(DictEncodedStrings::<Vec<u8>>::from_strings(&values, u));
        panic!("TODO")
    } else {
        Box::new(StringPacker::from_strings(&values))
    }
}

struct StringPacker {
    data: Vec<u8>,
}

// TODO: encode using variable size length + special value to represent null
impl StringPacker {
    pub fn new() -> StringPacker {
        StringPacker { data: Vec::new() }
    }

    pub fn from_strings(strings: &Vec<Option<Rc<String>>>) -> StringPacker {
        let mut sp = StringPacker::new();
        for string in strings {
            match string {
                &Some(ref string) => sp.push(string),
                &None => sp.push(""),
            }
        }
        sp.shrink_to_fit();
        sp
    }

    pub fn push(&mut self, string: &str) {
        for &byte in string.as_bytes().iter() {
            self.data.push(byte);
        }
        self.data.push(0);
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub fn iter(&self) -> StringPackerIterator {
        StringPackerIterator { data: &self.data, curr_index: 0 }
    }
}

impl ColumnData for StringPacker {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.iter().map(|s| ValueType::Str(s));
        ColIter{iter: Box::new(iter)}
    }
}

impl HeapSizeOf for StringPacker {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

pub struct StringPackerIterator<'a> {
    data: &'a Vec<u8>,
    curr_index: usize,
}

impl<'a> Iterator for StringPackerIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.curr_index >= self.data.len() { return None }

        let mut index = self.curr_index;
        while self.data[index] != 0 {
            index += 1;
        }
        let result = unsafe {
            str::from_utf8_unchecked(&self.data[self.curr_index..index])
        };
        self.curr_index = index + 1;
        Some(result)
    }
}

trait PackedStore<'a, T>: HeapSizeOf + FromIterator<T> {
    type Iter: Iterator<Item=T> + 'a;
    fn iter(&'a self) -> Self::Iter;
}

impl<'a, T> PackedStore<'a, T> for Vec<T> where T: Copy + HeapSizeOf + 'a {
    type Iter = iter::Cloned<slice::Iter<'a, T>>;

    fn iter(&'a self) -> Self::Iter {
        self.deref().iter().cloned()
    }
}

struct SomeStruct<'a, T: 'a, I: PackedStore<'a, T>> {
    storage: I,
    phantom: PhantomData<&'a T>,
}

impl<'a, T: 'a, I: PackedStore<'a, T>> SomeStruct<'a, T, I> {
    fn f(&'a self) -> Box<Iterator<Item=T> + 'a> {
        Box::new(self.storage.iter())
    }
}

fn test<'a>() {
    let specialized1 = SomeStruct { storage: Vec::<u8>::new(), phantom: PhantomData };
    let specialized1 = SomeStruct { storage: Vec::<u16>::new(), phantom: PhantomData };
}


struct DictEncodedStrings<S> {
    mapping: Vec<Option<String>>,
    encoded_values: S,
}

// impl<S: IndexStorage> DictEncodedStrings<S> {
//     pub fn from_strings(strings: &Vec<Option<Rc<String>>>, unique_values: HashSet<Option<Rc<String>>>) -> DictEncodedStrings<S> {
//         assert!(unique_values.len() <= u16::MAX as usize);

//         let mapping: Vec<Option<String>> = unique_values.into_iter().map(|o| o.map(|s| s.as_str().to_owned())).collect();
//         let encoded_values: S = {
//             let reverse_mapping: HashMap<Option<&String>, usize> = mapping.iter().map(Option::as_ref).zip(0..).collect();
//             strings.iter().map(|o| reverse_mapping[&o.as_ref().map(|x| &**x)]).collect()
//         };

//         // println!("\tMapping: {}MB; values: {}MB",
//         //          mapping.heap_size_of_children() as f64 / 1024f64 / 1024f64,
//         //          encoded_values.heap_size_of_children() as f64 / 1024f64 / 1024f64);

//         DictEncodedStrings { mapping: mapping, encoded_values: encoded_values }
//     }
// }

pub struct DictEncodedStringsIterator<'a, S> where S: 'a {
    data: &'a DictEncodedStrings<S>,
    i: usize,
}

// impl<'a, S: IndexStorage> Iterator for DictEncodedStringsIterator<'a, S> {
//     type Item = Option<&'a str>;

//     fn next(&mut self) -> Option<Option<&'a str>> {
//         if self.i >= self.data.encoded_values.len() { return None }
//         let encoded_value = &self.data.encoded_values[self.i];
//         let value = &self.data.mapping[*encoded_value as usize];

//         self.i += 1;
//         Some(value.as_ref().map(|s| &**s))
//     }
// }

// impl<S: IndexStorage> ColumnData for DictEncodedStrings<S> {
//     fn iter<'a>(&'a self) -> ColIter<'a> {
//         // let iter = self.encoded_values.iter().map(|i| &self.mapping[*i as usize]).map(|o| o.as_ref().map(|x| &**x)).map(ValueType::from); 
//         let iter = DictEncodedStringsIterator { data: self, i: 0 }.map(ValueType::from);
//         ColIter{iter: Box::new(iter)}
//     }
// }

// impl<S: IndexStorage> HeapSizeOf for DictEncodedStrings<S> {
//     fn heap_size_of_children(&self) -> usize {
//         self.mapping.heap_size_of_children() + self.encoded_values.heap_size_of_children()
//     }
// }

