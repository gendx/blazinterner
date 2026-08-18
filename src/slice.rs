use crate::Index;
use alloc::vec::Vec;
#[cfg(feature = "sync")]
use appendvec::AppendVec;
use core::borrow::Borrow;
#[cfg(feature = "serde")]
use core::cell::Cell;
use core::cmp::Ordering;
use core::fmt::Debug;
use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;
use core::ops::Range;
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
use serde::ser::{SerializeSeq, SerializeTuple};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct CopyRange<I> {
    pub start: I,
    pub end: I,
}

impl<I> From<Range<I>> for CopyRange<I> {
    fn from(other: Range<I>) -> Self {
        Self {
            start: other.start,
            end: other.end,
        }
    }
}

impl<I> From<CopyRange<I>> for Range<I> {
    fn from(other: CopyRange<I>) -> Self {
        other.start..other.end
    }
}

#[cfg(feature = "serde")]
impl<I> Serialize for CopyRange<I>
where
    I: Serialize + Copy,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (self.start, self.end).serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, I> Deserialize<'de> for CopyRange<I>
where
    I: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (start, end) = <(I, I)>::deserialize(deserializer)?;
        Ok(Self { start, end })
    }
}

/// A handle to an interned value in an [`ArenaSlice`].
#[cfg_attr(feature = "get-size2", derive(GetSize))]
pub struct InternedSlice<T, H = DefaultHashBuilder, I = u32> {
    id: I,
    _phantom: PhantomData<fn() -> (*const T, H)>,
}

impl<T, H, I: Index> Default for InternedSlice<T, H, I> {
    fn default() -> Self {
        Self::new(I::MAX)
    }
}

impl<T, H, I: Index> Debug for InternedSlice<T, H, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T, H, I: Index> Clone for InternedSlice<T, H, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, H, I: Index> Copy for InternedSlice<T, H, I> {}

impl<T, H, I: Index> PartialEq for InternedSlice<T, H, I> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T, H, I: Index> Eq for InternedSlice<T, H, I> {}

impl<T, H, I: Index> PartialOrd for InternedSlice<T, H, I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, H, I: Index> Ord for InternedSlice<T, H, I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T, H, I: Index> Hash for InternedSlice<T, H, I> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        self.id.hash(state);
    }
}

#[cfg(feature = "raw")]
impl<T, H, I> InternedSlice<T, H, I> {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`ArenaSlice::intern()`] API to intern a value, unless you really know
    /// what you're doing.
    pub fn from_id(id: I) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "raw")]
impl<T, H, I: Index> InternedSlice<T, H, I> {
    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`ArenaSlice::lookup()`] API, unless you really know what you're doing.
    pub fn id(&self) -> I {
        self.id
    }
}

impl<T, H, I> InternedSlice<T, H, I> {
    pub(crate) fn new(id: I) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T, H, I: Index> InternedSlice<T, H, I> {
    pub(crate) fn id_(&self) -> I {
        self.id
    }
}

#[cfg(feature = "serde")]
impl<T, H, I> Serialize for InternedSlice<T, H, I>
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
impl<'de, T, H, I> Deserialize<'de> for InternedSlice<T, H, I>
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
struct RangeVec<T, I> {
    #[cfg(not(feature = "sync"))]
    vec: Vec<T>,
    #[cfg(feature = "sync")]
    vec: AppendVec<T>,
    #[cfg(not(feature = "sync"))]
    ranges: Vec<CopyRange<I>>,
    #[cfg(feature = "sync")]
    ranges: AppendVec<CopyRange<I>>,
}

impl<T, I: Index> RangeVec<T, I> {
    fn lookup_slice(&self, id: I) -> &[T] {
        let range = self.ranges[id.to_usize()];
        let range = range.start.to_usize()..range.end.to_usize();
        &self.vec[range]
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = &[T]> {
        self.ranges
            .iter()
            .map(|&range| &self.vec[range.start.to_usize()..range.end.to_usize()])
    }

    #[cfg(feature = "sync")]
    fn push_range(&self, range: Range<usize>) -> I {
        assert!(range.start <= I::MAX.to_usize());
        assert!(range.end <= I::MAX.to_usize());
        let range = I::from_usize(range.start)..I::from_usize(range.end);

        let id = self.ranges.push(range.into());
        assert!(id <= I::MAX.to_usize());
        I::from_usize(id)
    }

    fn push_range_mut(&mut self, range: Range<usize>) -> I {
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

/// Interning arena for slices of type `T`.
pub struct ArenaSlice<T, H = DefaultHashBuilder, I = u32> {
    rangevec: RangeVec<T, I>,
    #[cfg(not(feature = "sync"))]
    map: HashTable<I>,
    #[cfg(feature = "sync")]
    map: DashTable<I>,
    hasher: H,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
}

impl<T, H, I> Clone for ArenaSlice<T, H, I>
where
    T: Default + Clone + Eq + Hash,
    H: Default + BuildHasher,
    I: Index,
{
    fn clone(&self) -> Self {
        let iter = self.iter_();
        let mut arena = Self::with_capacity(iter.len(), self.items());
        for slice in iter {
            arena.push(slice);
        }
        arena
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    H: Default,
{
    /// Creates a new arena with pre-allocated space to store at least the given
    /// number of slices, totalling the given number of items of type `T`.
    pub fn with_capacity(slices: usize, items: usize) -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            rangevec: RangeVec {
                vec: Vec::with_capacity(items),
                ranges: Vec::with_capacity(slices),
            },
            #[cfg(feature = "sync")]
            rangevec: RangeVec {
                vec: AppendVec::with_capacity(items),
                ranges: AppendVec::with_capacity(slices),
            },
            #[cfg(not(feature = "sync"))]
            map: HashTable::with_capacity(slices),
            #[cfg(feature = "sync")]
            map: DashTable::with_capacity(slices),
            hasher: H::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
        }
    }
}

impl<T, H, I> ArenaSlice<T, H, I> {
    /// Returns the number of slices in this arena.
    ///
    /// Note that because [`ArenaSlice`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn slices(&self) -> usize {
        self.rangevec.ranges.len()
    }

    /// Returns the total number of items of type `T` in this arena.
    ///
    /// Note that because [`ArenaSlice`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn items(&self) -> usize {
        self.rangevec.vec.len()
    }

    /// Checks if this arena is empty.
    ///
    /// Note that because [`ArenaSlice`] is a concurrent data structure, this is
    /// only a snapshot as viewed by this thread, and the result may change
    /// if other threads are inserting values.
    pub fn is_empty(&self) -> bool {
        self.slices() == 0
    }
}

impl<T, H, I: Index> ArenaSlice<T, H, I> {
    /// Returns an iterator over all slices in this arena, in indexing order.
    ///
    /// Note that because [`ArenaSlice`] is a concurrent data structure, this is
    /// only a snapshot. Once this iterator has been created, for performance
    /// reasons it will not iterate over items added afterwards, even on the
    /// same thread.
    #[cfg(feature = "raw")]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &[T]> {
        self.rangevec.iter()
    }

    fn iter_(&self) -> impl ExactSizeIterator<Item = &[T]> {
        self.rangevec.iter()
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Eq + Hash,
    H: BuildHasher,
    I: Index,
{
    /// Returns the given value's [`InternedSlice`] handle if it is already
    /// interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// See also [`find_mut()`](Self::find_mut), which is more efficient if you
    /// hold a mutable reference to this arena as it avoids acquiring locks.
    pub fn find(&self, value: &[T]) -> Option<InternedSlice<T, H, I>> {
        let hash = self.hash_slice(value);
        self.map
            .find(hash, |&i| self.lookup_slice(i) == value)
            .map(|id| InternedSlice::new(*id))
    }

    /// Returns the given value's [`InternedSlice`] handle if it is already
    /// interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// Contrary to [`find()`](Self::find), no locks are held internally because
    /// this function already takes an exclusive mutable reference to this
    /// arena.
    pub fn find_mut(&mut self, value: &[T]) -> Option<InternedSlice<T, H, I>> {
        let hash = self.hash_slice(value);
        #[cfg(not(feature = "sync"))]
        return self
            .map
            .find(hash, |&i| self.lookup_slice(i) == value)
            .map(|id| InternedSlice::new(*id));
        #[cfg(feature = "sync")]
        return self
            .map
            .find_mut(hash, |&i| self.rangevec.lookup_slice(i) == value)
            .map(|id| InternedSlice::new(*id));
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Hash,
    H: BuildHasher,
{
    fn hash_slice(&self, slice: &[T]) -> u64 {
        Self::hash_iter(&self.hasher, slice)
    }

    fn hash_iter(hasher: &H, iter: impl IntoIterator<Item = impl Borrow<T>>) -> u64 {
        let mut hasher = hasher.build_hasher();
        for t in iter.into_iter() {
            t.borrow().hash(&mut hasher);
        }
        hasher.finish()
    }
}

impl<T, H, I> Default for ArenaSlice<T, H, I>
where
    H: Default,
{
    fn default() -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            rangevec: RangeVec {
                vec: Vec::new(),
                ranges: Vec::new(),
            },
            #[cfg(feature = "sync")]
            rangevec: RangeVec {
                vec: AppendVec::new(),
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

impl<T, H, I: Index> Debug for ArenaSlice<T, H, I>
where
    T: Debug,
{
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt.debug_list().entries(self.iter_()).finish()
    }
}

impl<T, H, I> PartialEq for ArenaSlice<T, H, I>
where
    T: Eq,
    I: Index,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter_().eq(other.iter_())
    }
}

impl<T, H, I: Index> Eq for ArenaSlice<T, H, I> where T: Eq {}

#[cfg(feature = "get-size2")]
impl<T, H, I> GetSize for ArenaSlice<T, H, I>
where
    T: GetSize,
    I: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let (size_vec, tracker) = GetSize::get_heap_size_with_tracker(&self.rangevec, tracker);
        let (size_map, tracker) = GetSize::get_heap_size_with_tracker(&self.map, tracker);
        (size_vec + size_map, tracker)
    }
}

#[cfg(all(feature = "debug", feature = "std"))]
impl<T, H, I> ArenaSlice<T, H, I>
where
    T: GetSize,
{
    /// Prints a summary of the storage used by this arena to stdout.
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let slices = self.rangevec.ranges.len();
        let items = self.rangevec.vec.len();
        let references = self.references();
        let estimated_bytes = self.get_size();
        println!(
            "{}[{:.02}%] {} interner: {} objects | {} items ({:.02} items/object) | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            prefix,
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            title,
            slices,
            items,
            items as f64 / slices as f64,
            estimated_bytes,
            estimated_bytes as f64 / slices as f64,
            references,
            references as f64 / slices as f64,
        );
    }
}

#[cfg(feature = "debug")]
impl<T, H, I> ArenaSlice<T, H, I> {
    /// Returns the total number of references to slices in this arena.
    ///
    /// The underlying counter is incremented each time a slice is interned,
    /// whether it was already previously in the arena or not.
    pub fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Eq,
{
    // TODO: use Iterator::eq_by when feature(iter_order_by) becomes stable.
    fn iter_eq<'a>(
        mut lhs: impl ExactSizeIterator<Item = &'a T>,
        mut rhs: impl ExactSizeIterator<Item = T>,
    ) -> bool
    where
        T: 'a,
    {
        if lhs.len() != rhs.len() {
            return false;
        }
        loop {
            match (lhs.next(), rhs.next()) {
                (None, None) => return true,
                (Some(_), None) | (None, Some(_)) => return false,
                (Some(x), Some(y)) => {
                    if *x != y {
                        return false;
                    }
                }
            }
        }
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Default + Eq + Hash,
    H: BuildHasher,
    I: Index,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// See also [`intern_owned_mut()`](Self::intern_owned_mut), which is more
    /// efficient if you hold a mutable reference to this arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern_owned(&self, value: Vec<T>) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hash_slice(&value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.lookup_slice(i) == value,
                |&i| self.hash_slice(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                let range = self.rangevec.vec.push_owned_slice(value);
                self.rangevec.push_range(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// Contrary to [`intern_owned()`](Self::intern_owned), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this arena.
    pub fn intern_owned_mut(&mut self, value: Vec<T>) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(&value);
        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        let id = *entry
            .or_insert_with(|| {
                #[cfg(not(feature = "sync"))]
                let range = {
                    let start = self.rangevec.vec.len();
                    let mut value = value;
                    self.rangevec.vec.append(&mut value);
                    drop(value);
                    let end = self.rangevec.vec.len();
                    start..end
                };
                #[cfg(feature = "sync")]
                let range = self.rangevec.vec.push_owned_slice_mut(value);
                self.rangevec.push_range_mut(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// See also [`intern_array_mut()`](Self::intern_array_mut), which is more
    /// efficient if you hold a mutable reference to this arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern_array<const N: usize>(&self, value: [T; N]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hash_slice(&value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.lookup_slice(i) == value,
                |&i| self.hash_slice(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                let range = self.rangevec.vec.push_array(value);
                self.rangevec.push_range(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// Contrary to [`intern_array()`](Self::intern_array), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this arena.
    pub fn intern_array_mut<const N: usize>(&mut self, value: [T; N]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(&value);
        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        let id = *entry
            .or_insert_with(|| {
                #[cfg(not(feature = "sync"))]
                let range = {
                    let start = self.rangevec.vec.len();
                    self.rangevec.vec.extend(value);
                    let end = self.rangevec.vec.len();
                    start..end
                };
                #[cfg(feature = "sync")]
                let range = self.rangevec.vec.push_array_mut(value);
                self.rangevec.push_range_mut(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given iterator (collected as a slice) in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// See also [`intern_iter_mut()`](Self::intern_iter_mut), which is more
    /// efficient if you hold a mutable reference to this arena as it avoids
    /// acquiring locks.
    ///
    /// # Safety
    ///
    /// This function requires the iterator length to be correct (and not to
    /// change upon cloning). This is akin to the nightly-only
    /// [`TrustedLen`](core::iter::TrustedLen) trait.
    #[cfg(feature = "sync")]
    pub unsafe fn intern_iter(
        &self,
        value: impl ExactSizeIterator<Item = T> + Clone,
    ) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = Self::hash_iter(&self.hasher, value.clone());

        let id = *self
            .map
            .entry(
                hash,
                |&i| {
                    let lhs = self.lookup_slice(i).iter();
                    let rhs = value.clone();
                    Self::iter_eq(lhs, rhs)
                },
                |&i| self.hash_slice(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                // SAFETY: The caller ensures that the iterator length is correct.
                let range = unsafe { self.rangevec.vec.push_contiguous(value) };
                self.rangevec.push_range(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given iterator (collected as a slice) in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// Contrary to [`intern_iter()`](Self::intern_iter), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this arena.
    ///
    /// # Safety
    ///
    /// This function requires the iterator length to be correct (and not to
    /// change upon cloning). This is akin to the nightly-only
    /// [`TrustedLen`](core::iter::TrustedLen) trait.
    pub unsafe fn intern_iter_mut(
        &mut self,
        value: impl ExactSizeIterator<Item = T> + Clone,
    ) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = Self::hash_iter(&self.hasher, value.clone());

        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| {
                let lhs = self.rangevec.lookup_slice(i).iter();
                let rhs = value.clone();
                Self::iter_eq(lhs, rhs)
            },
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| {
                let lhs = self.rangevec.lookup_slice(i).iter();
                let rhs = value.clone();
                Self::iter_eq(lhs, rhs)
            },
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        let id = *entry
            .or_insert_with(|| {
                #[cfg(not(feature = "sync"))]
                let range = {
                    let start = self.rangevec.vec.len();
                    self.rangevec.vec.extend(value);
                    let end = self.rangevec.vec.len();
                    start..end
                };
                // SAFETY: The caller ensures that the iterator length is correct.
                #[cfg(feature = "sync")]
                let range = unsafe { self.rangevec.vec.push_contiguous_mut(value) };
                self.rangevec.push_range_mut(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    #[cfg(feature = "raw")]
    pub fn push_owned_mut(&mut self, value: Vec<T>) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(&value);

        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.rangevec.vec.len();
            let mut value = value;
            self.rangevec.vec.append(&mut value);
            drop(value);
            let end = self.rangevec.vec.len();
            start..end
        };
        #[cfg(feature = "sync")]
        let range = self.rangevec.vec.push_owned_slice_mut(value);
        let id = self.rangevec.push_range_mut(range);

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        InternedSlice::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    #[cfg(feature = "raw")]
    pub fn push_array_mut<const N: usize>(&mut self, value: [T; N]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(&value);

        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.rangevec.vec.len();
            self.rangevec.vec.extend(value);
            let end = self.rangevec.vec.len();
            start..end
        };
        #[cfg(feature = "sync")]
        let range = self.rangevec.vec.push_array_mut(value);
        let id = self.rangevec.push_range_mut(range);

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        InternedSlice::new(id)
    }

    /// Unconditionally push an iterator as a single contiguous value, without
    /// validating that it's already interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// # Safety
    ///
    /// This function requires the iterator length to be correct. This is akin
    /// to the nightly-only [`TrustedLen`](core::iter::TrustedLen) trait.
    #[cfg(feature = "raw")]
    pub unsafe fn push_iter_mut(
        &mut self,
        value: impl ExactSizeIterator<Item = T>,
    ) -> InternedSlice<T, H, I> {
        // SAFETY: Simply forwarding to the crate-internal version.
        unsafe { self.push_iter_mut_(value) }
    }

    /// Unconditionally push an iterator as a single contiguous value, without
    /// validating that it's already interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// # Safety
    ///
    /// This function requires the iterator length to be correct. This is akin
    /// to the nightly-only [`TrustedLen`](core::iter::TrustedLen) trait.
    pub(crate) unsafe fn push_iter_mut_(
        &mut self,
        value: impl ExactSizeIterator<Item = T>,
    ) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.rangevec.vec.len();
            self.rangevec.vec.extend(value);
            let end = self.rangevec.vec.len();
            start..end
        };
        // SAFETY: The caller ensures that the iterator length is correct.
        #[cfg(feature = "sync")]
        let range = unsafe { self.rangevec.vec.push_contiguous_mut(value) };
        let id = self.rangevec.push_range_mut(range.clone());

        let hash = self.hash_slice(&self.rangevec.vec[range]);

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        InternedSlice::new(id)
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Default + Clone + Eq + Hash,
    H: BuildHasher,
    I: Index,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// If `T` is [`Copy`], it may be more efficient to call
    /// [`intern_copy()`](Self::intern_copy) instead.
    ///
    /// See also [`intern_mut()`](Self::intern_mut), which is more efficient if
    /// you hold a mutable reference to this arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern(&self, value: &[T]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hash_slice(value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.lookup_slice(i) == value,
                |&i| self.hash_slice(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                let range = self.rangevec.vec.push_slice(value);
                self.rangevec.push_range(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// If `T` is [`Copy`], it may be more efficient to call
    /// [`intern_copy_mut()`](Self::intern_copy_mut) instead.
    ///
    /// Contrary to [`intern()`](Self::intern), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// this arena.
    pub fn intern_mut(&mut self, value: &[T]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(value);
        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| self.rangevec.lookup_slice(i) == value,
            |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
        );
        let id = *entry
            .or_insert_with(|| {
                #[cfg(not(feature = "sync"))]
                let range = {
                    let start = self.rangevec.vec.len();
                    self.rangevec.vec.extend_from_slice(value);
                    let end = self.rangevec.vec.len();
                    start..end
                };
                #[cfg(feature = "sync")]
                let range = self.rangevec.vec.push_slice_mut(value);
                self.rangevec.push_range_mut(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// If `T` is [`Copy`], calling [`push_copy_mut()`](Self::push_copy_mut) may
    /// be more efficient.
    #[cfg(feature = "raw")]
    pub fn push_mut(&mut self, value: &[T]) -> I {
        self.push(value)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    pub(crate) fn push(&mut self, value: &[T]) -> I {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(value);

        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.rangevec.vec.len();
            self.rangevec.vec.extend_from_slice(value);
            let end = self.rangevec.vec.len();
            start..end
        };
        #[cfg(feature = "sync")]
        let range = self.rangevec.vec.push_slice_mut(value);
        let id = self.rangevec.push_range_mut(range);

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        id
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Default + Copy + Eq + Hash,
    H: BuildHasher,
    I: Index,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// If `T` is only [`Clone`], you can call [`intern()`](Self::intern)
    /// instead. If `T` is also not [`Clone`], you can call
    /// [`intern_owned()`](Self::intern_owned) or
    /// [`intern_array()`](Self::intern_array).
    ///
    /// See also [`intern_copy_mut()`](Self::intern_copy_mut), which is more
    /// efficient if you hold a mutable reference to this arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern_copy(&self, value: &[T]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hash_slice(value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.lookup_slice(i) == value,
                |&i| self.hash_slice(self.lookup_slice(i)),
            )
            .or_insert_with(|| {
                let range = self.rangevec.vec.push_slice_copy(value);
                self.rangevec.push_range(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, its interning index
    /// will simply be returned. Otherwise it will be stored into the arena.
    ///
    /// If `T` is only [`Clone`], you can call
    /// [`intern_mut()`](Self::intern_mut) instead. If `T` is also not
    /// [`Clone`], you can call [`intern_owned_mut()`](Self::intern_owned_mut)
    /// or [`intern_array_mut()`](Self::intern_array_mut).
    ///
    /// Contrary to [`intern_copy()`](Self::intern_copy), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this arena.
    pub fn intern_copy_mut(&mut self, value: &[T]) -> InternedSlice<T, H, I> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(value);
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.rangevec.lookup_slice(i) == value,
                |&i| Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i)),
            )
            .or_insert_with(|| {
                #[cfg(not(feature = "sync"))]
                let range = {
                    let start = self.rangevec.vec.len();
                    self.rangevec.vec.extend_from_slice(value);
                    let end = self.rangevec.vec.len();
                    start..end
                };
                #[cfg(feature = "sync")]
                let range = self.rangevec.vec.push_slice_copy_mut(value);
                self.rangevec.push_range_mut(range)
            })
            .get();
        InternedSlice::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// If `T` is only [`Clone`], you can call [`push_mut()`](Self::push_mut)
    /// instead.
    #[cfg(feature = "raw")]
    pub fn push_copy_mut(&mut self, value: &[T]) -> I {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hash_slice(value);

        #[cfg(not(feature = "sync"))]
        let range = {
            let start = self.rangevec.vec.len();
            self.rangevec.vec.extend_from_slice(value);
            let end = self.rangevec.vec.len();
            start..end
        };
        #[cfg(feature = "sync")]
        let range = self.rangevec.vec.push_slice_copy_mut(value);
        let id = self.rangevec.push_range_mut(range);

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            Self::hash_iter(&self.hasher, self.rangevec.lookup_slice(i))
        });
        id
    }
}

impl<T, H, I: Index> ArenaSlice<T, H, I> {
    /// Retrieves the given [`InternedSlice`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    pub fn lookup(&self, interned: InternedSlice<T, H, I>) -> &[T] {
        self.lookup_slice(interned.id)
    }

    fn lookup_slice(&self, id: I) -> &[T] {
        self.rangevec.lookup_slice(id)
    }
}

#[cfg(feature = "serde")]
impl<T, H, I> Serialize for ArenaSlice<T, H, I>
where
    T: Serialize,
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

        tuple.serialize_element(&ArenaSliceWrapper {
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
struct ArenaSliceWrapper<'a, T, I> {
    ranges_len: I,
    total_len: I,
    rangevec: &'a RangeVec<T, I>,
}

#[cfg(feature = "serde")]
impl<'a, T, I> Serialize for ArenaSliceWrapper<'a, T, I>
where
    T: Serialize,
    I: Index,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.total_len.to_usize()))?;

        for range in self.rangevec.ranges.iter().take(self.ranges_len.to_usize()) {
            let slice = &self.rangevec.vec[range.start.to_usize()..range.end.to_usize()];
            for t in slice {
                seq.serialize_element(t)?;
            }
        }

        seq.end()
    }
}

#[cfg(feature = "serde")]
impl<'de, T, H, I> Deserialize<'de> for ArenaSlice<T, H, I>
where
    T: Default + Clone + Eq + Hash + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, ArenaSliceVisitor::new())
    }
}

#[cfg(feature = "serde")]
struct ArenaSliceVisitor<T, H, I> {
    #[expect(clippy::type_complexity)]
    _phantom: PhantomData<fn() -> ArenaSlice<T, H, I>>,
}

#[cfg(feature = "serde")]
impl<T, H, I> ArenaSliceVisitor<T, H, I> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, T, H, I> Visitor<'de> for ArenaSliceVisitor<T, H, I>
where
    T: Default + Clone + Eq + Hash + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index + Deserialize<'de>,
{
    type Value = ArenaSlice<T, H, I>;

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
        let values: Vec<T> = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;

        let mut arena = ArenaSlice::with_capacity(sizes.len(), values.len());

        let mut start = 0;
        for size in sizes {
            let size = size.to_usize();
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
    use alloc::boxed::Box;

    impl<T, H, I, Delta, Accum> Serialize for DeltaEncoding<&ArenaSlice<T, H, I>, Accum>
    where
        I: Index + Serialize,
        Delta: Serialize,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, DeltaStorage = Box<[Delta]>>,
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

            tuple.serialize_element(&ArenaSliceWrapper {
                ranges_len: ranges.ranges_len.into_inner(),
                total_len: ranges.total_len.into_inner(),
                rangevec: &self.map_ref(|arena| &arena.rangevec),
            })?;

            tuple.end()
        }
    }

    struct ArenaSliceWrapper<'a, T, I, Accum> {
        ranges_len: I,
        total_len: I,
        rangevec: &'a DeltaEncoding<&'a RangeVec<T, I>, Accum>,
    }

    impl<'a, T, I, Delta, Accum> Serialize for ArenaSliceWrapper<'a, T, I, Accum>
    where
        I: Index,
        Delta: Serialize,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, DeltaStorage = Box<[Delta]>>,
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
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaSlice: delta length must match source slice length"
                );
                for d in delta {
                    seq.serialize_element(&d)?;
                }
            }

            seq.end()
        }
    }

    impl<'de, T, H, I, Delta, Accum> Deserialize<'de> for DeltaEncoding<ArenaSlice<T, H, I>, Accum>
    where
        T: Default + Copy + Eq + Hash,
        H: Default + BuildHasher,
        I: Index + Deserialize<'de>,
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

    struct DeltaArenaSliceVisitor<T, H, I, Accum> {
        #[expect(clippy::type_complexity)]
        _phantom: PhantomData<fn() -> ArenaSlice<T, H, I>>,
        _accum: PhantomData<Accum>,
    }

    impl<T, H, I, Accum> DeltaArenaSliceVisitor<T, H, I, Accum> {
        fn new() -> Self {
            Self {
                _phantom: PhantomData,
                _accum: PhantomData,
            }
        }
    }

    impl<'de, T, H, I, Delta, Accum> Visitor<'de> for DeltaArenaSliceVisitor<T, H, I, Accum>
    where
        T: Default + Copy + Eq + Hash,
        H: Default + BuildHasher,
        I: Index + Deserialize<'de>,
        Delta: Deserialize<'de>,
        Accum: Accumulator<Value = [T], Storage = Box<[T]>, Delta = [Delta]>,
    {
        type Value = DeltaEncoding<ArenaSlice<T, H, I>, Accum>;

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
            let values: Vec<Delta> = seq
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(1, &self))?;

            let mut arena = ArenaSlice::with_capacity(sizes.len(), values.len());

            let mut acc = Accum::default();
            let mut start = 0;
            for size in sizes {
                let size = size.to_usize();
                let delta = &values[start..start + size];
                let slice = acc.unfold(delta);
                assert_eq!(
                    delta.len(),
                    slice.len(),
                    "Invalid Accumulator implementation for DeltaEncoding of ArenaSlice: delta length must match destination slice length"
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_intern_mut_lookup() {
        let mut arena: ArenaSlice<u32> = ArenaSlice::default();
        for i in 0..100 {
            let slice = vec![i; i as usize];
            assert_eq!(arena.intern_mut(&slice).id, i);
        }
        for i in 0..100 {
            let slice = arena.lookup(InternedSlice::new(i));
            let expected = vec![i; i as usize];
            assert_eq!(slice, &expected);
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
        let mut arena: ArenaSlice<u32> = ArenaSlice::default();
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                let slice = vec![j; j as usize];
                assert_eq!(arena.push_mut(&slice), i * NUM_VALUES + j);
                let id = arena.intern_mut(&slice).id;
                assert_eq!(id % NUM_VALUES, j);
                assert!(id / NUM_VALUES <= i);
            }
        }
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                let slice = arena.lookup(InternedSlice::new(i * NUM_VALUES + j));
                let expected = vec![j; j as usize];
                assert_eq!(slice, &expected);
            }
        }
    }
}
