#[cfg(feature = "serde")]
use super::U32Visitor;
use appendvec::AppendVec;
use dashtable::DashTable;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use hashbrown::DefaultHashBuilder;
#[cfg(feature = "serde")]
use serde::de::{Error, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::ser::{SerializeSeq, SerializeTuple};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "serde")]
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
#[cfg(feature = "serde")]
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Range;
#[cfg(feature = "debug")]
use std::sync::atomic::{self, AtomicUsize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize_tuple, Deserialize_tuple))]
struct CopyRangeU32 {
    pub start: u32,
    pub end: u32,
}

impl From<Range<u32>> for CopyRangeU32 {
    fn from(other: Range<u32>) -> Self {
        CopyRangeU32 {
            start: other.start,
            end: other.end,
        }
    }
}

impl From<CopyRangeU32> for Range<u32> {
    fn from(other: CopyRangeU32) -> Self {
        other.start..other.end
    }
}

/// A handle to an interned value in an [`ArenaSlice`].
pub struct InternedSlice<T> {
    id: u32,
    _phantom: PhantomData<fn() -> *const T>,
}

impl<T> Debug for InternedSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T> Clone for InternedSlice<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for InternedSlice<T> {}

impl<T> PartialEq for InternedSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T> Eq for InternedSlice<T> {}

impl<T> PartialOrd for InternedSlice<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for InternedSlice<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T> Hash for InternedSlice<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

#[cfg(feature = "get-size2")]
impl<T> GetSize for InternedSlice<T> {
    // There is nothing on the heap, so the default implementation works out of the
    // box.
}

#[cfg(feature = "raw")]
impl<T> InternedSlice<T> {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`from()`](Self::from) API to intern a value, unless you really know
    /// what you're doing.
    pub fn from_id(id: u32) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`lookup()`](Self::lookup) API, unless you really know what you're
    /// doing.
    pub fn id(&self) -> u32 {
        self.id
    }
}

impl<T> InternedSlice<T>
where
    T: Default + Copy + Eq + Hash,
{
    /// Interns the given value in the given [`ArenaSlice`].
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    pub fn from(arena: &ArenaSlice<T>, value: &[T]) -> Self {
        let id = arena.intern(value);
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T> InternedSlice<T> {
    /// Retrieves this interned value from the given [`ArenaSlice`].
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    pub fn lookup<'a>(&self, arena: &'a ArenaSlice<T>) -> &'a [T] {
        arena.lookup_slice(self.id)
    }
}

#[cfg(feature = "serde")]
impl<T> Serialize for InternedSlice<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.id)
    }
}

#[cfg(feature = "serde")]
impl<'de, T> Deserialize<'de> for InternedSlice<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = deserializer.deserialize_u32(U32Visitor)?;
        Ok(Self {
            id,
            _phantom: PhantomData,
        })
    }
}

/// Interning arena for slices of type `T`.
pub struct ArenaSlice<T> {
    vec: AppendVec<T>,
    ranges: AppendVec<CopyRangeU32>,
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
}

impl<T> ArenaSlice<T> {
    fn iter(&self) -> impl Iterator<Item = &[T]> {
        self.ranges
            .iter()
            .map(|&range| &self.vec[range.start as usize..range.end as usize])
    }
}

impl<T> Default for ArenaSlice<T> {
    fn default() -> Self {
        Self {
            vec: AppendVec::new(),
            ranges: AppendVec::new(),
            map: DashTable::new(),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }
}

impl<T> Debug for ArenaSlice<T>
where
    T: Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_list().entries(self.iter()).finish()
    }
}

impl<T> PartialEq for ArenaSlice<T>
where
    T: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T> Eq for ArenaSlice<T> where T: Eq + Hash {}

#[cfg(feature = "get-size2")]
impl<T> GetSize for ArenaSlice<T>
where
    T: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let heap_size = self.vec.iter().map(|x| x.get_size()).sum::<usize>()
            + self.ranges.len() * (size_of::<CopyRangeU32>() + size_of::<u32>());
        (heap_size, tracker)
    }
}

#[cfg(feature = "debug")]
impl<T> ArenaSlice<T>
where
    T: GetSize,
{
    /// Prints a summary of the storage used by this arena to stdout.
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let num_items = self.num_items();
        let num_slices = self.num_slices();
        let references = self.references();
        let estimated_bytes = self.get_size();
        println!(
            "{}[{:.02}%] {} interner: {} objects | {} items ({:.02} items/object) | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            prefix,
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            title,
            num_slices,
            num_items,
            num_items as f64 / num_slices as f64,
            estimated_bytes,
            estimated_bytes as f64 / num_slices as f64,
            references,
            references as f64 / num_slices as f64,
        );
    }
}

#[cfg(feature = "debug")]
impl<T> ArenaSlice<T> {
    fn num_items(&self) -> usize {
        self.vec.len()
    }

    fn num_slices(&self) -> usize {
        self.ranges.len()
    }

    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T> ArenaSlice<T>
where
    T: Default + Copy + Eq + Hash,
{
    fn intern(&self, value: &[T]) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value);
        *self
            .map
            .entry(
                hash,
                |&i| self.lookup_slice(i) == value,
                |&i| self.hasher.hash_one(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                let range = self.vec.push_slice(value);
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
    #[cfg(feature = "serde")]
    fn push(&mut self, value: &[T]) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value);

        let range = self.vec.push_slice(value);
        assert!(range.start <= u32::MAX as usize);
        assert!(range.end <= u32::MAX as usize);
        let range = range.start as u32..range.end as u32;

        let id = self.ranges.push(range.into());
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        self.map
            .insert_unique(hash, id, |&i| self.hasher.hash_one(self.lookup_slice(i)));

        id
    }
}

impl<T> ArenaSlice<T> {
    fn lookup_slice(&self, id: u32) -> &[T] {
        let range = self.ranges[id as usize];
        let range = range.start as usize..range.end as usize;
        &self.vec[range]
    }
}

#[cfg(feature = "serde")]
impl<T> Serialize for ArenaSlice<T>
where
    T: Serialize,
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

        tuple.serialize_element(&ArenaSliceWrapper {
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
struct ArenaSliceWrapper<'a, T> {
    ranges_len: u32,
    total_len: u32,
    arena: &'a ArenaSlice<T>,
}

#[cfg(feature = "serde")]
impl<'a, T> Serialize for ArenaSliceWrapper<'a, T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.total_len as usize))?;

        for range in self.arena.ranges.iter().take(self.ranges_len as usize) {
            let slice = &self.arena.vec[range.start as usize..range.end as usize];
            for t in slice {
                seq.serialize_element(t)?;
            }
        }

        seq.end()
    }
}

#[cfg(feature = "serde")]
impl<'de, T> Deserialize<'de> for ArenaSlice<T>
where
    T: Default + Copy + Eq + Hash + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, ArenaSliceVisitor::new())
    }
}

#[cfg(feature = "serde")]
struct ArenaSliceVisitor<T> {
    _phantom: PhantomData<fn() -> ArenaSlice<T>>,
}

#[cfg(feature = "serde")]
impl<T> ArenaSliceVisitor<T> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, T> Visitor<'de> for ArenaSliceVisitor<T>
where
    T: Default + Copy + Eq + Hash + Deserialize<'de>,
{
    type Value = ArenaSlice<T>;

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
        let values: Vec<T> = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;

        let mut arena = ArenaSlice {
            vec: AppendVec::with_capacity(values.len()),
            ranges: AppendVec::with_capacity(sizes.len()),
            map: DashTable::with_capacity(sizes.len()),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        };

        let mut start = 0;
        for size in sizes {
            let size = size as usize;
            arena.push(&values[start..start + size]);
            start += size;
        }

        Ok(arena)
    }
}

#[cfg(all(feature = "delta", feature = "serde"))]
mod delta {
    use super::*;
    use crate::{Accumulator, DeltaEncoding};

    impl<T, Delta, Accum> Serialize for DeltaEncoding<ArenaSlice<T>, Accum>
    where
        Delta: Serialize,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, DeltaStorage = Box<[Delta]>>,
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

            tuple.serialize_element(&ArenaSliceWrapper {
                ranges_len: ranges.ranges_len.into_inner(),
                total_len: ranges.total_len.into_inner(),
                arena: self,
            })?;

            tuple.end()
        }
    }

    struct ArenaSliceWrapper<'a, T, Accum> {
        ranges_len: u32,
        total_len: u32,
        arena: &'a DeltaEncoding<ArenaSlice<T>, Accum>,
    }

    impl<'a, T, Delta, Accum> Serialize for ArenaSliceWrapper<'a, T, Accum>
    where
        Delta: Serialize,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, DeltaStorage = Box<[Delta]>>,
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
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaSlice: delta length must match source slice length"
                );
                for d in delta {
                    seq.serialize_element(&d)?;
                }
            }

            seq.end()
        }
    }

    impl<'de, T, Delta, Accum> Deserialize<'de> for DeltaEncoding<ArenaSlice<T>, Accum>
    where
        T: Default + Copy + Eq + Hash,
        Delta: Deserialize<'de>,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, Delta = [Delta]>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(2, DeltaArenaSliceVisitor::new())
        }
    }

    struct DeltaArenaSliceVisitor<T, Accum> {
        _phantom: PhantomData<fn() -> ArenaSlice<T>>,
        _accum: PhantomData<Accum>,
    }

    impl<T, Accum> DeltaArenaSliceVisitor<T, Accum> {
        fn new() -> Self {
            Self {
                _phantom: PhantomData,
                _accum: PhantomData,
            }
        }
    }

    impl<'de, T, Delta, Accum> Visitor<'de> for DeltaArenaSliceVisitor<T, Accum>
    where
        T: Default + Copy + Eq + Hash,
        Delta: Deserialize<'de>,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, Delta = [Delta]>,
    {
        type Value = DeltaEncoding<ArenaSlice<T>, Accum>;

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
            let values: Vec<Delta> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;

            let mut arena = ArenaSlice {
                vec: AppendVec::with_capacity(values.len()),
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
                let delta = &values[start..start + size];
                let slice = acc.unfold(delta);
                assert_eq!(
                    delta.len(),
                    slice.len(),
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaSlice: delta length must match source slice length"
                );
                arena.push(&slice);
                start += size;
            }

            Ok(DeltaEncoding {
                inner: arena,
                _phantom: PhantomData,
            })
        }
    }
}
