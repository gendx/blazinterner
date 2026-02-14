use crate::CopyRangeU32;
use appendvec::{AppendStr, AppendVec};
use dashtable::DashTable;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use hashbrown::DefaultHashBuilder;
#[cfg(feature = "serde")]
use serde::de::{Error, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::ser::SerializeTuple;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "serde")]
use std::cell::Cell;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
#[cfg(feature = "debug")]
use std::sync::atomic::{self, AtomicUsize};

/// A handle to an interned value in an [`ArenaStr`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct InternedStr(u32);

impl Debug for InternedStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("I").field(&self.0).finish()
    }
}

#[cfg(feature = "raw")]
impl InternedStr {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`from()`](Self::from) API to intern a value, unless you really know
    /// what you're doing.
    pub fn from_id(id: u32) -> Self {
        Self(id)
    }

    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`lookup()`](Self::lookup) API, unless you really know what you're
    /// doing.
    pub fn id(&self) -> u32 {
        self.0
    }
}

impl InternedStr {
    /// Interns the given value in the given [`ArenaStr`].
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    pub fn from(arena: &ArenaStr, value: &str) -> Self {
        let id = arena.intern(value);
        Self(id)
    }

    /// Retrieves this interned value from the given [`ArenaStr`].
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    pub fn lookup<'a>(&self, arena: &'a ArenaStr) -> &'a str {
        arena.lookup_str(self.0)
    }
}

/// Interning arena for strings.
pub struct ArenaStr {
    vec: AppendStr,
    ranges: AppendVec<CopyRangeU32>,
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
}

impl Clone for ArenaStr {
    fn clone(&self) -> Self {
        let mut arena = Self::with_capacity(self.strings(), self.bytes());
        for s in self.iter() {
            arena.push(s);
        }
        arena
    }
}

impl ArenaStr {
    /// Creates a new arena with pre-allocated space to store at least the given
    /// number of strings, totalling the given number of bytes.
    pub fn with_capacity(strings: usize, bytes: usize) -> Self {
        Self {
            vec: AppendStr::with_capacity(bytes),
            ranges: AppendVec::with_capacity(strings),
            map: DashTable::with_capacity(strings),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }

    /// Returns the number of strings in this arena.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn strings(&self) -> usize {
        self.ranges.len()
    }

    /// Returns the total number of bytes in this arena.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn bytes(&self) -> usize {
        self.vec.len()
    }

    /// Checks if this arena is empty.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn is_empty(&self) -> bool {
        self.strings() == 0
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    pub fn push_mut(&mut self, value: &str) -> u32 {
        self.push(value)
    }
}

impl ArenaStr {
    fn iter(&self) -> impl Iterator<Item = &str> {
        self.ranges
            .iter()
            .map(|&range| &self.vec[range.start as usize..range.end as usize])
    }
}

impl Default for ArenaStr {
    fn default() -> Self {
        Self {
            vec: AppendStr::new(),
            ranges: AppendVec::new(),
            map: DashTable::new(),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }
}

impl Debug for ArenaStr {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ArenaStr {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ArenaStr {}

#[cfg(feature = "get-size2")]
impl GetSize for ArenaStr {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let heap_size = self.vec.len() * size_of::<u8>()
            + self.ranges.len() * (size_of::<CopyRangeU32>() + size_of::<u32>());
        (heap_size, tracker)
    }
}

#[cfg(feature = "debug")]
impl ArenaStr {
    /// Prints a summary of the storage used by this arena to stdout.
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let strings = self.ranges.len();
        let references = self.references();
        let estimated_bytes = self.get_size();
        println!(
            "{}[{:.02}%] {} interner: {} objects | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            prefix,
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            title,
            strings,
            estimated_bytes,
            estimated_bytes as f64 / strings as f64,
            references,
            references as f64 / strings as f64,
        );
    }

    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl ArenaStr {
    fn intern(&self, value: &str) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value);
        *self
            .map
            .entry(
                hash,
                |&i| self.lookup_str(i) == value,
                |&i| self.hasher.hash_one(self.lookup_str(i)),
            )
            .or_insert_with(|| {
                let range = self.vec.push_str(value);
                assert!(range.start <= u32::MAX as usize);
                assert!(range.end <= u32::MAX as usize);
                let range = range.start as u32..range.end as u32;

                let id = self.ranges.push(range.into());
                assert!(id <= u32::MAX as usize);
                id as u32
            })
            .get()
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    fn push(&mut self, value: &str) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value);

        let range = self.vec.push_str_mut(value);
        assert!(range.start <= u32::MAX as usize);
        assert!(range.end <= u32::MAX as usize);
        let range = range.start as u32..range.end as u32;

        let id = self.ranges.push_mut(range.into());
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        self.map
            .insert_unique(hash, id, |&i| self.hasher.hash_one(self.lookup_str(i)));

        id
    }

    fn lookup_str(&self, id: u32) -> &str {
        let range = self.ranges[id as usize];
        let range = range.start as usize..range.end as usize;
        &self.vec[range]
    }
}

#[cfg(feature = "serde")]
impl Serialize for ArenaStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(2)?;

        let ranges = RangeWrapper {
            ranges: &self.ranges,
            ranges_len: Cell::new(0),
            total_len: Cell::new(0),
        };
        tuple.serialize_element(&ranges)?;

        tuple.serialize_element(&ArenaStrWrapper {
            ranges_len: ranges.ranges_len.into_inner(),
            total_len: ranges.total_len.into_inner(),
            arena: self,
        })?;

        tuple.end()
    }
}

#[cfg(feature = "serde")]
struct RangeWrapper<'a> {
    ranges: &'a AppendVec<CopyRangeU32>,
    ranges_len: Cell<u32>,
    total_len: Cell<u32>,
}

#[cfg(feature = "serde")]
impl<'a> Serialize for RangeWrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ranges_len: u32 = 0;
        let mut total_len: u32 = 0;
        let result = serializer.collect_seq(self.ranges.iter().map(|range| {
            ranges_len += 1;
            let this_len = range.end - range.start;
            total_len = total_len.strict_add(this_len);
            this_len
        }));

        self.ranges_len.set(ranges_len);
        self.total_len.set(total_len);

        result
    }
}

#[cfg(feature = "serde")]
struct ArenaStrWrapper<'a> {
    ranges_len: u32,
    total_len: u32,
    arena: &'a ArenaStr,
}

#[cfg(feature = "serde")]
impl<'a> Serialize for ArenaStrWrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO: Make this zero-copy?
        let mut string = String::with_capacity(self.total_len as usize);
        for range in self.arena.ranges.iter().take(self.ranges_len as usize) {
            let s = &self.arena.vec[range.start as usize..range.end as usize];
            string.push_str(s);
        }

        serializer.serialize_str(&string)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for ArenaStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, ArenaStrVisitor)
    }
}

#[cfg(feature = "serde")]
struct ArenaStrVisitor;

#[cfg(feature = "serde")]
impl<'de> Visitor<'de> for ArenaStrVisitor {
    type Value = ArenaStr;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a pair of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let sizes: Vec<u32> = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let string: &str = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;

        let mut arena = ArenaStr {
            vec: AppendStr::with_capacity(string.len()),
            ranges: AppendVec::with_capacity(sizes.len()),
            map: DashTable::with_capacity(sizes.len()),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        };

        let mut start = 0;
        for size in sizes {
            let size = size as usize;
            arena.push(&string[start..start + size]);
            start += size;
        }

        Ok(arena)
    }
}

#[cfg(all(feature = "delta", feature = "serde"))]
mod delta {
    use super::*;
    use crate::{Accumulator, DeltaEncoding};
    use serde::ser::SerializeSeq;
    use std::marker::PhantomData;

    impl<Accum> Serialize for DeltaEncoding<&ArenaStr, Accum>
    where
        Accum: Accumulator<Value = str, Storage = Box<str>, DeltaStorage = Box<[u8]>>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(2)?;

            let ranges = RangeWrapper {
                ranges: &self.ranges,
                ranges_len: Cell::new(0),
                total_len: Cell::new(0),
            };
            tuple.serialize_element(&ranges)?;

            tuple.serialize_element(&ArenaStrWrapper {
                ranges_len: ranges.ranges_len.into_inner(),
                total_len: ranges.total_len.into_inner(),
                arena: self,
            })?;

            tuple.end()
        }
    }

    struct ArenaStrWrapper<'a, Accum> {
        ranges_len: u32,
        total_len: u32,
        arena: &'a DeltaEncoding<&'a ArenaStr, Accum>,
    }

    impl<'a, Accum> Serialize for ArenaStrWrapper<'a, Accum>
    where
        Accum: Accumulator<Value = str, Storage = Box<str>, DeltaStorage = Box<[u8]>>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.total_len as usize))?;

            let mut acc = Accum::default();
            for range in self.arena.ranges.iter().take(self.ranges_len as usize) {
                let slice = &self.arena.vec[range.start as usize..range.end as usize];
                let delta = acc.fold(slice);
                assert_eq!(
                    delta.len(),
                    slice.len(),
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaStr: delta length must match source string length (in bytes)"
                );
                for d in delta {
                    seq.serialize_element(&d)?;
                }
            }

            seq.end()
        }
    }

    impl<'de, Accum> Deserialize<'de> for DeltaEncoding<ArenaStr, Accum>
    where
        Accum: Accumulator<Value = str, Storage = Box<str>, Delta = [u8]>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(2, DeltaArenaStrVisitor::new())
        }
    }

    struct DeltaArenaStrVisitor<Accum> {
        _accum: PhantomData<Accum>,
    }

    impl<Accum> DeltaArenaStrVisitor<Accum> {
        fn new() -> Self {
            Self {
                _accum: PhantomData,
            }
        }
    }

    impl<'de, Accum> Visitor<'de> for DeltaArenaStrVisitor<Accum>
    where
        Accum: Accumulator<Value = str, Storage = Box<str>, Delta = [u8]>,
    {
        type Value = DeltaEncoding<ArenaStr, Accum>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a pair of values")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let sizes: Vec<u32> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(0, &self))?;
            let bytes: &[u8] = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;

            let mut arena = ArenaStr {
                vec: AppendStr::with_capacity(bytes.len()),
                ranges: AppendVec::with_capacity(sizes.len()),
                map: DashTable::with_capacity(sizes.len()),
                hasher: DefaultHashBuilder::default(),
                #[cfg(feature = "debug")]
                references: AtomicUsize::new(0),
            };

            let mut acc = Accum::default();
            let mut start = 0;
            for size in sizes {
                let size = size as usize;
                let delta = &bytes[start..start + size];
                let string = acc.unfold(delta);
                assert_eq!(
                    delta.len(),
                    string.len(),
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaSlice: delta length must match destination string length (in bytes)"
                );
                arena.push(&string);
                start += size;
            }

            Ok(DeltaEncoding {
                inner: arena,
                _phantom: PhantomData,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[cfg(all(feature = "delta", feature = "serde"))]
    use crate::{Accumulator, DeltaEncoding};
    use std::thread;

    fn make_utf8_string(mut i: u32) -> String {
        let mut s = String::new();
        while i != 0 {
            let j = i % (64 + 26);
            let c = if j < 64 {
                // See https://en.wikipedia.org/wiki/Cyrillic_script_in_Unicode.
                char::from_u32(0x410 + j).expect("Invalid Unicode value")
            } else {
                char::from_u32(b'a' as u32 + j - 64).expect("Invalid Unicode value")
            };
            i /= 64 + 26;
            s.push(c);
        }
        s
    }

    #[test]
    fn test_utf8_string() {
        assert_eq!(make_utf8_string(0), "");
        assert_eq!(make_utf8_string(0).len(), 0);
        assert_eq!(make_utf8_string(5), "Е");
        assert_eq!(make_utf8_string(5).len(), 2);
        assert_eq!(make_utf8_string(25), "Щ");
        assert_eq!(make_utf8_string(25).len(), 2);
        assert_eq!(make_utf8_string(125), "гБ");
        assert_eq!(make_utf8_string(125).len(), 4);
        assert_eq!(make_utf8_string(625), "vЖ");
        assert_eq!(make_utf8_string(625).len(), 3);
        assert_eq!(make_utf8_string(3125), "bв");
        assert_eq!(make_utf8_string(3125).len(), 3);
        assert_eq!(make_utf8_string(15625), "чtБ");
        assert_eq!(make_utf8_string(15625).len(), 5);
        assert_eq!(make_utf8_string(78125), "ЕъЙ");
        assert_eq!(make_utf8_string(78125).len(), 6);
        assert_eq!(make_utf8_string(390625), "ЩФр");
        assert_eq!(make_utf8_string(390625).len(), 6);
        assert_eq!(make_utf8_string(1953125), "гЛэВ");
        assert_eq!(make_utf8_string(1953125).len(), 8);
        assert_eq!(make_utf8_string(9765625), "vшгН");
        assert_eq!(make_utf8_string(9765625).len(), 7);
    }

    #[test]
    fn test_lookup() {
        let arena = ArenaStr::default();

        let empty = InternedStr::from(&arena, "");
        let a = InternedStr::from(&arena, "a");
        let b = InternedStr::from(&arena, "bb");
        let c = InternedStr::from(&arena, "ccc");
        let d = InternedStr::from(&arena, "dddd");
        let e = InternedStr::from(&arena, "eeeee");

        assert_eq!(empty.lookup(&arena), "");
        assert_eq!(a.lookup(&arena), "a");
        assert_eq!(b.lookup(&arena), "bb");
        assert_eq!(c.lookup(&arena), "ccc");
        assert_eq!(d.lookup(&arena), "dddd");
        assert_eq!(e.lookup(&arena), "eeeee");
    }

    #[test]
    fn test_intern_lookup() {
        let arena = ArenaStr::default();
        for i in 0..100 {
            assert_eq!(arena.intern(&make_utf8_string(i)), i);
        }
        for i in 0..100 {
            assert_eq!(arena.lookup_str(i), &make_utf8_string(i));
        }
    }

    const NUM_READERS: usize = 4;
    const NUM_WRITERS: usize = 4;
    #[cfg(not(miri))]
    const NUM_ITEMS: usize = 1_000_000;
    #[cfg(miri)]
    const NUM_ITEMS: usize = 100;

    #[test]
    fn test_intern_lookup_concurrent_reads() {
        let arena = ArenaStr::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.strings();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(arena.lookup_str(last), &make_utf8_string(last));
                            if len == NUM_ITEMS {
                                break;
                            }
                        }
                    }
                });
            }
            s.spawn(|| {
                for j in 0..NUM_ITEMS as u32 {
                    assert_eq!(arena.intern(&make_utf8_string(j)), j);
                }
            });
        });
    }

    #[test]
    fn test_intern_lookup_concurrent_writes() {
        let arena = ArenaStr::default();
        thread::scope(|s| {
            s.spawn(|| {
                loop {
                    let len = arena.strings();
                    if len > 0 {
                        let last = len as u32 - 1;
                        assert_eq!(arena.lookup_str(last), &make_utf8_string(last));
                        if len == NUM_ITEMS {
                            break;
                        }
                    }
                }
            });
            for _ in 0..NUM_WRITERS {
                s.spawn(|| {
                    for j in 0..NUM_ITEMS as u32 {
                        assert_eq!(arena.intern(&make_utf8_string(j)), j);
                    }
                });
            }
        });
    }

    #[test]
    fn test_intern_lookup_concurrent_readwrites() {
        let arena = ArenaStr::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.strings();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(arena.lookup_str(last), &make_utf8_string(last));
                            if len == NUM_ITEMS {
                                break;
                            }
                        }
                    }
                });
            }
            for _ in 0..NUM_WRITERS {
                s.spawn(|| {
                    for j in 0..NUM_ITEMS as u32 {
                        assert_eq!(arena.intern(&make_utf8_string(j)), j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_postcard() {
        let arena = ArenaStr::default();

        let empty = InternedStr::from(&arena, "");
        let a = InternedStr::from(&arena, "a");
        let b = InternedStr::from(&arena, "bb");
        let c = InternedStr::from(&arena, "ccc");
        let d = InternedStr::from(&arena, "dddd");
        let e = InternedStr::from(&arena, "eeeee");

        assert_eq!(arena.strings(), 6);
        assert!(arena.bytes() >= 15);

        let serialized_arena = postcard::to_stdvec(&arena).expect("Failed to serialize arena");
        assert_eq!(
            serialized_arena,
            vec![
                6, 0, 1, 2, 3, 4, 5, 15, b'a', b'b', b'b', b'c', b'c', b'c', b'd', b'd', b'd',
                b'd', b'e', b'e', b'e', b'e', b'e'
            ]
        );
        let new_arena: ArenaStr =
            postcard::from_bytes(&serialized_arena).expect("Failed to deserialize arena");
        assert_eq!(new_arena, arena);

        assert_eq!(new_arena.strings(), 6);
        assert_eq!(new_arena.bytes(), 15);

        let serialized_handles = postcard::to_stdvec(&[empty, a, b, c, d, e])
            .expect("Failed to serialize interned handles");
        assert_eq!(serialized_handles, vec![0, 1, 2, 3, 4, 5]);
        let new_handles: [InternedStr; 6] = postcard::from_bytes(&serialized_handles)
            .expect("Failed to deserialize interned handles");
        assert_eq!(new_handles, [empty, a, b, c, d, e]);

        assert_eq!(empty.lookup(&new_arena), "");
        assert_eq!(a.lookup(&new_arena), "a");
        assert_eq!(b.lookup(&new_arena), "bb");
        assert_eq!(c.lookup(&new_arena), "ccc");
        assert_eq!(d.lookup(&new_arena), "dddd");
        assert_eq!(e.lookup(&new_arena), "eeeee");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_json() {
        let arena = ArenaStr::default();

        let empty = InternedStr::from(&arena, "");
        let a = InternedStr::from(&arena, "a");
        let b = InternedStr::from(&arena, "bb");
        let c = InternedStr::from(&arena, "ccc");
        let d = InternedStr::from(&arena, "dddd");
        let e = InternedStr::from(&arena, "eeeee");

        assert_eq!(arena.strings(), 6);
        assert!(arena.bytes() >= 15);

        let serialized_arena = serde_json::to_string(&arena).expect("Failed to serialize arena");
        assert_eq!(serialized_arena, r#"[[0,1,2,3,4,5],"abbcccddddeeeee"]"#);
        let new_arena: ArenaStr =
            serde_json::from_str(&serialized_arena).expect("Failed to deserialize arena");
        assert_eq!(new_arena, arena);

        assert_eq!(new_arena.strings(), 6);
        assert_eq!(new_arena.bytes(), 15);

        let serialized_handles = serde_json::to_string(&[empty, a, b, c, d, e])
            .expect("Failed to serialize interned handles");
        assert_eq!(serialized_handles, "[0,1,2,3,4,5]");
        let new_handles: [InternedStr; 6] = serde_json::from_str(&serialized_handles)
            .expect("Failed to deserialize interned handles");
        assert_eq!(new_handles, [empty, a, b, c, d, e]);
    }

    #[cfg(all(feature = "delta", feature = "serde"))]
    #[derive(Default)]
    struct StringAccumulator {
        previous: Vec<u8>,
    }

    #[cfg(all(feature = "delta", feature = "serde"))]
    impl Accumulator for StringAccumulator {
        type Value = str;
        type Storage = Box<str>;
        type Delta = [u8];
        type DeltaStorage = Box<[u8]>;

        fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage {
            let mut delta = Vec::with_capacity(v.len());
            for (i, byte) in v.bytes().enumerate() {
                delta.push(byte ^ self.previous.get(i).copied().unwrap_or(0));
            }
            self.previous = v.into();
            delta.into()
        }

        fn unfold(&mut self, d: &Self::Delta) -> Self::Storage {
            let mut value = Vec::with_capacity(d.len());
            for (i, byte) in d.iter().enumerate() {
                value.push(byte ^ self.previous.get(i).copied().unwrap_or(0));
            }
            self.previous = value.clone();
            String::from_utf8(value)
                .expect("Invalid UTF-8 encoding")
                .into()
        }
    }

    #[cfg(all(feature = "delta", feature = "serde"))]
    #[test]
    fn test_serde_delta() {
        let arena = ArenaStr::default();

        let empty = InternedStr::from(&arena, "");
        let a = InternedStr::from(&arena, "a");
        let b = InternedStr::from(&arena, "bb");
        let c = InternedStr::from(&arena, "ccc");
        let d = InternedStr::from(&arena, "dddd");
        let e = InternedStr::from(&arena, "eeeee");

        assert_eq!(arena.strings(), 6);
        assert!(arena.bytes() >= 15);

        let delta_encoded: DeltaEncoding<&ArenaStr, StringAccumulator> = DeltaEncoding::new(&arena);
        let serialized_arena =
            postcard::to_stdvec(&delta_encoded).expect("Failed to serialize arena");
        assert_eq!(
            serialized_arena,
            vec![
                6, 0, 1, 2, 3, 4, 5, 15, 97, 3, 98, 1, 1, 99, 7, 7, 7, 100, 1, 1, 1, 1, 101
            ]
        );
        let delta_encoded: DeltaEncoding<ArenaStr, StringAccumulator> =
            postcard::from_bytes(&serialized_arena).expect("Failed to deserialize arena");
        let new_arena = delta_encoded.into_inner();

        assert_eq!(new_arena.strings(), 6);
        assert_eq!(new_arena.bytes(), 15);

        let serialized_handles = postcard::to_stdvec(&[empty, a, b, c, d, e])
            .expect("Failed to serialize interned handles");
        assert_eq!(serialized_handles, vec![0, 1, 2, 3, 4, 5]);
        let new_handles: [InternedStr; 6] = postcard::from_bytes(&serialized_handles)
            .expect("Failed to deserialize interned handles");
        assert_eq!(new_handles, [empty, a, b, c, d, e]);

        assert_eq!(empty.lookup(&new_arena), "");
        assert_eq!(a.lookup(&new_arena), "a");
        assert_eq!(b.lookup(&new_arena), "bb");
        assert_eq!(c.lookup(&new_arena), "ccc");
        assert_eq!(d.lookup(&new_arena), "dddd");
        assert_eq!(e.lookup(&new_arena), "eeeee");
    }
}
