use crate::{CopyRange, Index};
#[cfg(any(feature = "serde", not(feature = "sync")))]
use alloc::string::String;
#[cfg(any(feature = "serde", not(feature = "sync")))]
use alloc::vec::Vec;
#[cfg(feature = "sync")]
use appendvec::{AppendStr, AppendVec};
#[cfg(feature = "serde")]
use core::cell::Cell;
use core::cmp::Ordering;
use core::fmt::Debug;
use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;
#[cfg(feature = "debug")]
use core::sync::atomic::{self, AtomicUsize};
#[cfg(feature = "sync")]
use dashtable::DashTable;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use hashbrown::DefaultHashBuilder;
#[cfg(not(feature = "sync"))]
use hashbrown::HashTable;
#[cfg(feature = "serde")]
use serde::de::{Error, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::ser::SerializeTuple;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(feature = "serde")]
use serde_cow::CowStr;

/// A handle to an interned value in an [`ArenaStr`].
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct InternedStr<H = DefaultHashBuilder, I = u32> {
    id: I,
    _phantom: PhantomData<fn() -> H>,
}

impl<H, I: Index> Default for InternedStr<H, I> {
    fn default() -> Self {
        Self::new(I::MAX)
    }
}

impl<H, I: Index> Debug for InternedStr<H, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<H, I: Index> Clone for InternedStr<H, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H, I: Index> Copy for InternedStr<H, I> {}

impl<H, I: Index> PartialEq for InternedStr<H, I> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<H, I: Index> Eq for InternedStr<H, I> {}

impl<H, I: Index> PartialOrd for InternedStr<H, I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<H, I: Index> Ord for InternedStr<H, I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<H, I: Index> Hash for InternedStr<H, I> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        self.id.hash(state);
    }
}

#[cfg(feature = "raw")]
impl<H, I> InternedStr<H, I> {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`ArenaStr::intern()`] API to intern a value, unless you really know
    /// what you're doing.
    pub fn from_id(id: I) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "raw")]
impl<H, I: Index> InternedStr<H, I> {
    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`ArenaStr::lookup()`] and [`ArenaStr::lookup_bytes()`] APIs, unless you
    /// really know what you're doing.
    pub fn id(&self) -> I {
        self.id
    }
}

impl<H, I> InternedStr<H, I> {
    pub(crate) fn new(id: I) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<H, I: Index> InternedStr<H, I> {
    pub(crate) fn id_(&self) -> I {
        self.id
    }
}

#[cfg(feature = "serde")]
impl<H, I> Serialize for InternedStr<H, I>
where
    I: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.id.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, H, I> Deserialize<'de> for InternedStr<H, I>
where
    I: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = I::deserialize(deserializer)?;
        Ok(Self::new(id))
    }
}

#[cfg_attr(feature = "get-size2", derive(GetSize))]
struct RangeVecStr<I> {
    #[cfg(not(feature = "sync"))]
    vec: String,
    #[cfg(feature = "sync")]
    vec: AppendStr,
    #[cfg(not(feature = "sync"))]
    ranges: Vec<CopyRange<I>>,
    #[cfg(feature = "sync")]
    ranges: AppendVec<CopyRange<I>>,
}

impl<I: Index> RangeVecStr<I> {
    fn lookup_bytes(&self, id: I) -> &[u8] {
        let range = self.ranges[id.to_usize()];
        let range = range.start.to_usize()..range.end.to_usize();
        #[cfg(not(feature = "sync"))]
        return &self.vec.as_bytes()[range];
        #[cfg(feature = "sync")]
        return self.vec.get_bytes(range);
    }

    fn lookup_str(&self, id: I) -> &str {
        let range = self.ranges[id.to_usize()];
        let range = range.start.to_usize()..range.end.to_usize();
        &self.vec[range]
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = &str> {
        self.ranges
            .iter()
            .map(|&range| &self.vec[range.start.to_usize()..range.end.to_usize()])
    }

    fn iter_bytes(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        #[cfg(not(feature = "sync"))]
        let bytes = self.vec.as_bytes();
        self.ranges.iter().map(|&range| {
            #[cfg(not(feature = "sync"))]
            return &bytes[range.start.to_usize()..range.end.to_usize()];
            #[cfg(feature = "sync")]
            return self
                .vec
                .get_bytes(range.start.to_usize()..range.end.to_usize());
        })
    }

    #[cfg(feature = "sync")]
    fn push_str(&self, value: &str) -> I {
        let range = self.vec.push_str(value);
        assert!(range.start <= I::MAX.to_usize());
        assert!(range.end <= I::MAX.to_usize());
        let range = I::from_usize(range.start)..I::from_usize(range.end);

        let id = self.ranges.push(range.into());
        assert!(id <= I::MAX.to_usize());
        I::from_usize(id)
    }

    fn push_str_mut(&mut self, value: &str) -> I {
        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.vec.len();
            self.vec.push_str(value);
            let end = self.vec.len();
            start..end
        };
        #[cfg(feature = "sync")]
        let range = self.vec.push_str_mut(value);
        assert!(range.start <= I::MAX.to_usize());
        assert!(range.end <= I::MAX.to_usize());
        let range = I::from_usize(range.start)..I::from_usize(range.end);

        #[cfg(not(feature = "sync"))]
        let id = {
            let id = self.ranges.len();
            self.ranges.push(range.into());
            id
        };
        #[cfg(feature = "sync")]
        let id = self.ranges.push_mut(range.into());
        assert!(id <= I::MAX.to_usize());
        I::from_usize(id)
    }
}

/// Interning arena for strings.
pub struct ArenaStr<H = DefaultHashBuilder, I = u32> {
    rangevec: RangeVecStr<I>,
    #[cfg(not(feature = "sync"))]
    map: HashTable<I>,
    #[cfg(feature = "sync")]
    map: DashTable<I>,
    hasher: H,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
}

impl<H, I> Clone for ArenaStr<H, I>
where
    H: Default + BuildHasher,
    I: Index,
{
    fn clone(&self) -> Self {
        let iter = self.iter_();
        let mut arena = Self::with_capacity(iter.len(), self.bytes());
        for s in iter {
            arena.push(s);
        }
        arena
    }
}

impl<H, I> ArenaStr<H, I>
where
    H: Default,
{
    /// Creates a new arena with pre-allocated space to store at least the given
    /// number of strings, totalling the given number of bytes.
    pub fn with_capacity(strings: usize, bytes: usize) -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            rangevec: RangeVecStr {
                vec: String::with_capacity(bytes),
                ranges: Vec::with_capacity(strings),
            },
            #[cfg(feature = "sync")]
            rangevec: RangeVecStr {
                vec: AppendStr::with_capacity(bytes),
                ranges: AppendVec::with_capacity(strings),
            },
            #[cfg(not(feature = "sync"))]
            map: HashTable::with_capacity(strings),
            #[cfg(feature = "sync")]
            map: DashTable::with_capacity(strings),
            hasher: H::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }
}

impl<H, I> ArenaStr<H, I> {
    /// Returns the number of strings in this arena.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn strings(&self) -> usize {
        self.rangevec.ranges.len()
    }

    /// Returns the total number of bytes in this arena.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn bytes(&self) -> usize {
        self.rangevec.vec.len()
    }

    /// Checks if this arena is empty.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn is_empty(&self) -> bool {
        self.strings() == 0
    }
}

impl<H, I: Index> ArenaStr<H, I> {
    /// Returns an iterator over all strings in this arena, in indexing order.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot. Once this iterator has been created, for performance
    /// reasons it will not iterate over items added afterwards, even on the
    /// same thread.
    ///
    /// If you only need to access byte slices,
    /// [`iter_bytes()`](Self::iter_bytes) may be more efficient.
    #[cfg(feature = "raw")]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &str> {
        self.rangevec.iter()
    }

    fn iter_(&self) -> impl ExactSizeIterator<Item = &str> {
        self.rangevec.iter()
    }

    /// Returns an iterator over all strings (viewed as byte slices) in this
    /// arena, in indexing order.
    ///
    /// Note that because [`ArenaStr`] is a concurrent data structure, this is
    /// only a snapshot. Once this iterator has been created, for performance
    /// reasons it will not iterate over items added afterwards, even on the
    /// same thread.
    #[cfg(feature = "raw")]
    pub fn iter_bytes(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.rangevec.iter_bytes()
    }

    fn iter_bytes_(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.rangevec.iter_bytes()
    }
}

impl<H, I> ArenaStr<H, I>
where
    H: BuildHasher,
    I: Index,
{
    /// Returns the given string's [`InternedStr`] handle if it is already
    /// interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the string to
    /// this arena.
    ///
    /// See also [`find_mut()`](Self::find_mut), which is more efficient if you
    /// hold a mutable reference to this arena as it avoids acquiring locks.
    pub fn find(&self, value: &str) -> Option<InternedStr<H, I>> {
        let hash = self.hasher.hash_one(value);
        self.map
            .find(hash, |&i| self.lookup_str(i) == value)
            .map(|id| InternedStr::new(*id))
    }

    /// Returns the given string's [`InternedStr`] handle if it is already
    /// interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the string to
    /// this arena.
    ///
    /// Contrary to [`find()`](Self::find), no locks are held internally because
    /// this function already takes an exclusive mutable reference to this
    /// arena.
    pub fn find_mut(&mut self, value: &str) -> Option<InternedStr<H, I>> {
        let hash = self.hasher.hash_one(value);
        #[cfg(not(feature = "sync"))]
        return self
            .map
            .find(hash, |&i| self.lookup_str(i) == value)
            .map(|id| InternedStr::new(*id));
        #[cfg(feature = "sync")]
        return self
            .map
            .find_mut(hash, |&i| self.rangevec.lookup_str(i) == value)
            .map(|id| InternedStr::new(*id));
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    #[cfg(feature = "raw")]
    pub fn push_mut(&mut self, value: &str) -> I {
        self.push(value)
    }
}

impl<H, I> Default for ArenaStr<H, I>
where
    H: Default,
{
    fn default() -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            rangevec: RangeVecStr {
                vec: String::new(),
                ranges: Vec::new(),
            },
            #[cfg(feature = "sync")]
            rangevec: RangeVecStr {
                vec: AppendStr::new(),
                ranges: AppendVec::new(),
            },
            #[cfg(not(feature = "sync"))]
            map: HashTable::new(),
            #[cfg(feature = "sync")]
            map: DashTable::new(),
            hasher: H::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }
}

impl<H, I: Index> Debug for ArenaStr<H, I> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt.debug_list().entries(self.iter_()).finish()
    }
}

impl<H, I: Index> PartialEq for ArenaStr<H, I> {
    fn eq(&self, other: &Self) -> bool {
        self.iter_bytes_().eq(other.iter_bytes_())
    }
}

impl<H, I: Index> Eq for ArenaStr<H, I> {}

#[cfg(feature = "get-size2")]
impl<H, I> GetSize for ArenaStr<H, I>
where
    I: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let (size_vec, tracker) = GetSize::get_heap_size_with_tracker(&self.rangevec, tracker);
        let (size_map, tracker) = GetSize::get_heap_size_with_tracker(&self.map, tracker);
        (size_vec + size_map, tracker)
    }
}

#[cfg(all(feature = "debug", feature = "std"))]
impl<H, I> ArenaStr<H, I> {
    /// Prints a summary of the storage used by this arena to stdout.
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let strings = self.rangevec.ranges.len();
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
}

#[cfg(feature = "debug")]
impl<H, I> ArenaStr<H, I> {
    /// Returns the total number of references to strings in this arena.
    ///
    /// The underlying counter is incremented each time a string is interned,
    /// whether it was already previously in the arena or not.
    pub fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<H, I> ArenaStr<H, I>
where
    H: BuildHasher,
    I: Index,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// See also [`intern_mut()`](Self::intern_mut), which is more efficient if
    /// you hold a mutable reference to this arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern(&self, value: &str) -> InternedStr<H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.lookup_str(i) == value,
                |&i| self.hasher.hash_one(self.lookup_str(i)),
            )
            .or_insert_with(|| self.rangevec.push_str(value))
            .get();
        InternedStr::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// Contrary to [`intern()`](Self::intern), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// this arena.
    pub fn intern_mut(&mut self, value: &str) -> InternedStr<H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hasher.hash_one(value);
        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| self.rangevec.lookup_str(i) == value,
            |&i| self.hasher.hash_one(self.rangevec.lookup_str(i)),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| self.rangevec.lookup_str(i) == value,
            |&i| self.hasher.hash_one(self.rangevec.lookup_str(i)),
        );
        let id = *entry
            .or_insert_with(|| self.rangevec.push_str_mut(value))
            .get();
        InternedStr::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    pub(crate) fn push(&mut self, value: &str) -> I {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hasher.hash_one(value);
        let id = self.rangevec.push_str_mut(value);
        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            self.hasher.hash_one(self.rangevec.lookup_str(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            self.hasher.hash_one(self.rangevec.lookup_str(i))
        });
        id
    }
}

impl<H, I: Index> ArenaStr<H, I> {
    /// Retrieves the given [`InternedStr`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// If you only need to access the bytes,
    /// [`lookup_bytes()`](Self::lookup_bytes) may be more efficient.
    pub fn lookup(&self, interned: InternedStr<H, I>) -> &str {
        self.lookup_str(interned.id)
    }

    /// Retrieves the bytes for the given [`InternedStr`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    pub fn lookup_bytes(&self, interned: InternedStr<H, I>) -> &[u8] {
        self.rangevec.lookup_bytes(interned.id)
    }

    fn lookup_str(&self, id: I) -> &str {
        self.rangevec.lookup_str(id)
    }
}

#[cfg(feature = "serde")]
impl<H, I> Serialize for ArenaStr<H, I>
where
    I: Index + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple = serializer.serialize_tuple(2)?;

        let ranges = RangeWrapper {
            ranges: &self.rangevec.ranges,
            ranges_len: Cell::new(I::ZERO),
            total_len: Cell::new(I::ZERO),
        };
        tuple.serialize_element(&ranges)?;

        tuple.serialize_element(&ArenaStrWrapper {
            ranges_len: ranges.ranges_len.into_inner(),
            total_len: ranges.total_len.into_inner(),
            rangevec: &self.rangevec,
        })?;

        tuple.end()
    }
}

#[cfg(feature = "serde")]
struct RangeWrapper<'a, I> {
    #[cfg(not(feature = "sync"))]
    ranges: &'a [CopyRange<I>],
    #[cfg(feature = "sync")]
    ranges: &'a AppendVec<CopyRange<I>>,
    ranges_len: Cell<I>,
    total_len: Cell<I>,
}

#[cfg(feature = "serde")]
impl<'a, I> Serialize for RangeWrapper<'a, I>
where
    I: Index + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut ranges_len = I::ZERO;
        let mut total_len = I::ZERO;
        let result = serializer.collect_seq(self.ranges.iter().map(|range| {
            ranges_len.incr();
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
struct ArenaStrWrapper<'a, I> {
    ranges_len: I,
    total_len: I,
    rangevec: &'a RangeVecStr<I>,
}

#[cfg(feature = "serde")]
impl<'a, I> Serialize for ArenaStrWrapper<'a, I>
where
    I: Index,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // TODO: Make this zero-copy?
        let mut string = String::with_capacity(self.total_len.to_usize());
        for range in self.rangevec.ranges.iter().take(self.ranges_len.to_usize()) {
            let s = &self.rangevec.vec[range.start.to_usize()..range.end.to_usize()];
            string.push_str(s);
        }

        serializer.serialize_str(&string)
    }
}

#[cfg(feature = "serde")]
impl<'de, H, I> Deserialize<'de> for ArenaStr<H, I>
where
    H: Default + BuildHasher,
    I: Index + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, ArenaStrVisitor::new())
    }
}

#[cfg(feature = "serde")]
struct ArenaStrVisitor<H, I> {
    _phantom: PhantomData<fn() -> ArenaStr<H, I>>,
}

#[cfg(feature = "serde")]
impl<H, I> ArenaStrVisitor<H, I> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, H, I> Visitor<'de> for ArenaStrVisitor<H, I>
where
    H: Default + BuildHasher,
    I: Index + Deserialize<'de>,
{
    type Value = ArenaStr<H, I>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a pair of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let sizes: Vec<I> = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let string: CowStr = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;

        let mut arena = ArenaStr::with_capacity(sizes.len(), string.0.len());

        let mut start = 0;
        for size in sizes {
            let size = size.to_usize();
            arena.push(&string.0[start..start + size]);
            start += size;
        }

        Ok(arena)
    }
}

#[cfg(all(feature = "delta", feature = "serde"))]
mod delta {
    use super::*;
    use crate::{Accumulator, DeltaEncoding};
    use alloc::boxed::Box;
    use core::marker::PhantomData;
    use serde::ser::SerializeSeq;
    use serde_cow::CowBytes;

    impl<H, I, Accum> Serialize for DeltaEncoding<&ArenaStr<H, I>, Accum>
    where
        I: Index + Serialize,
        Accum: Accumulator<Value = str, Storage = Box<str>, DeltaStorage = Box<[u8]>>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tuple = serializer.serialize_tuple(2)?;

            let ranges = RangeWrapper {
                ranges: &self.rangevec.ranges,
                ranges_len: Cell::new(I::ZERO),
                total_len: Cell::new(I::ZERO),
            };
            tuple.serialize_element(&ranges)?;

            tuple.serialize_element(&ArenaStrWrapper {
                ranges_len: ranges.ranges_len.into_inner(),
                total_len: ranges.total_len.into_inner(),
                rangevec: &self.map_ref(|arena| &arena.rangevec),
            })?;

            tuple.end()
        }
    }

    struct ArenaStrWrapper<'a, I, Accum> {
        ranges_len: I,
        total_len: I,
        rangevec: &'a DeltaEncoding<&'a RangeVecStr<I>, Accum>,
    }

    impl<'a, I, Accum> Serialize for ArenaStrWrapper<'a, I, Accum>
    where
        I: Index,
        Accum: Accumulator<Value = str, Storage = Box<str>, DeltaStorage = Box<[u8]>>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.total_len.to_usize()))?;

            let mut acc = Accum::default();
            for range in self.rangevec.ranges.iter().take(self.ranges_len.to_usize()) {
                let slice = &self.rangevec.vec[range.start.to_usize()..range.end.to_usize()];
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

    impl<'de, H, I, Accum> Deserialize<'de> for DeltaEncoding<ArenaStr<H, I>, Accum>
    where
        H: Default + BuildHasher,
        I: Index + Deserialize<'de>,
        Accum: Accumulator<Value = str, Storage = Box<str>, Delta = [u8]>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(2, DeltaArenaStrVisitor::new())
        }
    }

    struct DeltaArenaStrVisitor<H, I, Accum> {
        _phantom: PhantomData<fn() -> ArenaStr<H, I>>,
        _accum: PhantomData<Accum>,
    }

    impl<H, I, Accum> DeltaArenaStrVisitor<H, I, Accum> {
        fn new() -> Self {
            Self {
                _phantom: PhantomData,
                _accum: PhantomData,
            }
        }
    }

    impl<'de, H, I, Accum> Visitor<'de> for DeltaArenaStrVisitor<H, I, Accum>
    where
        H: Default + BuildHasher,
        I: Index + Deserialize<'de>,
        Accum: Accumulator<Value = str, Storage = Box<str>, Delta = [u8]>,
    {
        type Value = DeltaEncoding<ArenaStr<H, I>, Accum>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            formatter.write_str("a pair of values")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let sizes: Vec<I> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(0, &self))?;
            let bytes: CowBytes = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;

            let mut arena = ArenaStr::with_capacity(sizes.len(), bytes.0.len());

            let mut acc = Accum::default();
            let mut start = 0;
            for size in sizes {
                let size = size.to_usize();
                let delta = &bytes.0[start..start + size];
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
    #[cfg(feature = "sync")]
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
        let mut arena: ArenaStr = ArenaStr::default();

        let empty = arena.intern_mut("");
        let a = arena.intern_mut("a");
        let b = arena.intern_mut("bb");
        let c = arena.intern_mut("ccc");
        let d = arena.intern_mut("dddd");
        let e = arena.intern_mut("eeeee");

        assert_eq!(arena.lookup(empty), "");
        assert_eq!(arena.lookup(a), "a");
        assert_eq!(arena.lookup(b), "bb");
        assert_eq!(arena.lookup(c), "ccc");
        assert_eq!(arena.lookup(d), "dddd");
        assert_eq!(arena.lookup(e), "eeeee");
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup() {
        let arena: ArenaStr = ArenaStr::default();
        for i in 0..100 {
            assert_eq!(arena.intern(&make_utf8_string(i)).id, i);
        }
        for i in 0..100 {
            assert_eq!(arena.lookup(InternedStr::new(i)), &make_utf8_string(i));
        }
    }

    #[test]
    fn test_intern_mut_lookup() {
        let mut arena: ArenaStr = ArenaStr::default();
        for i in 0..100 {
            assert_eq!(arena.intern_mut(&make_utf8_string(i)).id, i);
        }
        for i in 0..100 {
            assert_eq!(arena.lookup(InternedStr::new(i)), &make_utf8_string(i));
        }
    }

    #[cfg(all(feature = "raw", not(miri)))]
    const NUM_ITERS: u32 = 100;
    #[cfg(all(feature = "raw", miri))]
    const NUM_ITERS: u32 = 20;
    #[cfg(all(feature = "raw", not(miri)))]
    const NUM_VALUES: u32 = 50;
    #[cfg(all(feature = "raw", miri))]
    const NUM_VALUES: u32 = 10;

    #[cfg(feature = "raw")]
    #[test]
    fn test_push_mut_same_value_works() {
        let mut arena: ArenaStr = ArenaStr::default();
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                let s = make_utf8_string(j);
                assert_eq!(arena.push_mut(&s), i * NUM_VALUES + j);
                let id = arena.intern_mut(&s).id;
                assert_eq!(id % NUM_VALUES, j);
                assert!(id / NUM_VALUES <= i);
            }
        }
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                assert_eq!(
                    arena.lookup(InternedStr::new(i * NUM_VALUES + j)),
                    &make_utf8_string(j)
                );
            }
        }
    }

    #[cfg(feature = "sync")]
    const NUM_READERS: usize = 4;
    #[cfg(feature = "sync")]
    const NUM_WRITERS: usize = 4;
    #[cfg(all(feature = "sync", not(miri)))]
    const NUM_ITEMS: usize = 1_000_000;
    #[cfg(all(feature = "sync", miri))]
    const NUM_ITEMS: usize = 100;

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_reads() {
        let arena: ArenaStr = ArenaStr::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.strings();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(
                                arena.lookup(InternedStr::new(last)),
                                &make_utf8_string(last)
                            );
                            if len == NUM_ITEMS {
                                break;
                            }
                        }
                    }
                });
            }
            s.spawn(|| {
                for j in 0..NUM_ITEMS as u32 {
                    assert_eq!(arena.intern(&make_utf8_string(j)).id, j);
                }
            });
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_writes() {
        let arena: ArenaStr = ArenaStr::default();
        thread::scope(|s| {
            s.spawn(|| {
                loop {
                    let len = arena.strings();
                    if len > 0 {
                        let last = len as u32 - 1;
                        assert_eq!(
                            arena.lookup(InternedStr::new(last)),
                            &make_utf8_string(last)
                        );
                        if len == NUM_ITEMS {
                            break;
                        }
                    }
                }
            });
            for _ in 0..NUM_WRITERS {
                s.spawn(|| {
                    for j in 0..NUM_ITEMS as u32 {
                        assert_eq!(arena.intern(&make_utf8_string(j)).id, j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_readwrites() {
        let arena: ArenaStr = ArenaStr::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.strings();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(
                                arena.lookup(InternedStr::new(last)),
                                &make_utf8_string(last)
                            );
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
                        assert_eq!(arena.intern(&make_utf8_string(j)).id, j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_postcard() {
        let mut arena: ArenaStr = ArenaStr::default();

        let empty = arena.intern_mut("");
        let a = arena.intern_mut("a");
        let b = arena.intern_mut("bb");
        let c = arena.intern_mut("ccc");
        let d = arena.intern_mut("dddd");
        let e = arena.intern_mut("eeeee");

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

        assert_eq!(new_arena.lookup(empty), "");
        assert_eq!(new_arena.lookup(a), "a");
        assert_eq!(new_arena.lookup(b), "bb");
        assert_eq!(new_arena.lookup(c), "ccc");
        assert_eq!(new_arena.lookup(d), "dddd");
        assert_eq!(new_arena.lookup(e), "eeeee");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_json() {
        let mut arena: ArenaStr = ArenaStr::default();

        let empty = arena.intern_mut("");
        let a = arena.intern_mut("a");
        let b = arena.intern_mut("bb");
        let c = arena.intern_mut("ccc");
        let d = arena.intern_mut("dddd");
        let e = arena.intern_mut("eeeee");

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
        let mut arena: ArenaStr = ArenaStr::default();

        let empty = arena.intern_mut("");
        let a = arena.intern_mut("a");
        let b = arena.intern_mut("bb");
        let c = arena.intern_mut("ccc");
        let d = arena.intern_mut("dddd");
        let e = arena.intern_mut("eeeee");

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

        assert_eq!(new_arena.lookup(empty), "");
        assert_eq!(new_arena.lookup(a), "a");
        assert_eq!(new_arena.lookup(b), "bb");
        assert_eq!(new_arena.lookup(c), "ccc");
        assert_eq!(new_arena.lookup(d), "dddd");
        assert_eq!(new_arena.lookup(e), "eeeee");
    }
}
