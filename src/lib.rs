//! This crate offers an efficient and concurrent
//! [interning](https://en.wikipedia.org/wiki/Interning_(computer_science)) API
//! over generic data.
//!
//! Here are its main features.
//!
//! - **Generic**: You can intern any data type that implements [`Hash`] and
//!   [`Eq`], not just strings. The interned type doesn't even have to be
//!   [`Sized`] (for example [`str`](prim@str)), as long as you provide a
//!   [`Sized`] storage type (such as `Box<str>`) that can be borrowed as the
//!   interned type.
//! - **Efficient**: Each [`Interned`] value contains only a 32-bit index. The
//!   corresponding [`Arena`] stores each value directly in an [`AppendVec`],
//!   plus the 32-bit index in a raw hash table ([`DashTable`]). To intern a
//!   value of type `T` using storage type `S`, you can pass any type that
//!   implements [`Borrow<T>`](Borrow) and [`Into<S>`](Into), which allows
//!   avoiding unnecessary copies. For example, in an `Arena<str, Box<str>>` you
//!   can intern many string types: `&str`, `String`, `Box<str>`, `Cow<'_,
//!   str>`, etc.
//! - **Concurrent**: The [`Arena`] is [`Sync`], and allows simultaneous reads
//!   and writes. More specifically, retrieving values via [`Arena::lookup()`]
//!   and [`Arena::lookup_ref()`] is always wait-free, even when a write happens
//!   concurrently! This is thanks to the underlying [`AppendVec`]
//!   implementation. However, only one write (using [`Arena::intern()`]) can
//!   happen at a time on a given arena, due to an exclusive write lock.

#![forbid(
    missing_docs,
    unsafe_op_in_unsafe_fn,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block,
    clippy::undocumented_unsafe_blocks
)]
#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

#[cfg(feature = "delta")]
mod delta;
mod index;
mod mapping;
mod slice;
mod str;

#[cfg(feature = "serde")]
use alloc::format;
#[cfg(not(feature = "sync"))]
use alloc::vec::Vec;
#[cfg(feature = "sync")]
use appendvec::AppendVec;
use core::borrow::Borrow;
use core::cmp::Ordering;
use core::fmt::Debug;
use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;
#[cfg(feature = "debug")]
use core::sync::atomic::{self, AtomicUsize};
#[cfg(feature = "sync")]
use dashtable::DashTable;
#[cfg(feature = "sync")]
use dashtable::{Entry, MutEntry};
#[cfg(feature = "delta")]
pub use delta::{Accumulator, DeltaEncoding};
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
#[cfg(not(feature = "sync"))]
use hashbrown::HashTable;
#[cfg(not(feature = "sync"))]
use hashbrown::hash_table::Entry;
pub use index::{Index, U16, U24, U32, U40, U48, U56, U64};
pub use mapping::{ForwardMapping, Mapping, ReverseMapping};
#[cfg(feature = "retain")]
pub use mapping::{RetainBuilder, RetainSliceBuilder, RetainStrBuilder};
#[cfg(feature = "postcard-schema")]
use postcard_schema::Schema;
#[cfg(feature = "postcard-schema")]
use postcard_schema::schema::{DataModelType, NamedType};
#[cfg(feature = "serde")]
use serde::de::Error;
#[cfg(feature = "serde")]
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use slice::CopyRange;
pub use slice::{ArenaSlice, InternedSlice, SnapshotSlice, SnapshotSliceMark};
#[cfg(feature = "serde")]
pub use slice::{ExtendArenaSlice, SnapshotSliceDiff};
#[cfg(feature = "serde")]
use slice::{RangeWrapper, SnapshotRangeWrapper};
#[cfg(feature = "std")]
use std::hash::RandomState;
pub use str::{ArenaStr, InternedStr, SnapshotStr, SnapshotStrMark};
#[cfg(feature = "serde")]
pub use str::{ExtendArenaStr, SnapshotStrDiff};

/// The default [`BuildHasher`] from the `hashbrown` crate.
pub type HashbrownBuildHasher = hashbrown::DefaultHashBuilder;

/// The default [`BuildHasher`] from the standard library.
#[cfg(feature = "std")]
pub type StdBuildHasher = RandomState;

/// The default [`BuildHasher`] used in this crate.
///
/// When the `std` feature is enabled, this is the standard library default
/// hasher, otherwise this is the `hashbrown` crate's default hasher.
#[cfg(not(feature = "std"))]
pub type DefaultBuildHasher = HashbrownBuildHasher;

/// The default [`BuildHasher`] used in this crate.
///
/// When the `std` feature is enabled, this is the standard library default
/// hasher, otherwise this is the `hashbrown` crate's default hasher.
#[cfg(feature = "std")]
pub type DefaultBuildHasher = StdBuildHasher;

/// A handle to an interned value in an [`Arena`].
///
/// This is generic over the logical value type `T` as well as its `Storage`
/// type, that needs to be [`Sized`]. For [`Sized`] values, `Storage = T` is a
/// good default that incurs no overhead. For non-[`Sized`] values such as
/// [`str`](prim@str), you need to specify a [`Sized`] storage type, such as
/// `Box<T>`.
pub struct Interned<T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    id: I,
    #[expect(clippy::type_complexity)]
    _phantom: PhantomData<fn() -> (*const T, *const Storage, H)>,
}

impl<T: ?Sized, Storage, H, I: Index> Default for Interned<T, Storage, H, I> {
    fn default() -> Self {
        Self::new(I::MAX)
    }
}

impl<T: ?Sized, Storage, H, I: Index> Debug for Interned<T, Storage, H, I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T: ?Sized, Storage, H, I: Index> Clone for Interned<T, Storage, H, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized, Storage, H, I: Index> Copy for Interned<T, Storage, H, I> {}

impl<T: ?Sized, Storage, H, I: Index> PartialEq for Interned<T, Storage, H, I> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T: ?Sized, Storage, H, I: Index> Eq for Interned<T, Storage, H, I> {}

impl<T: ?Sized, Storage, H, I: Index> PartialOrd for Interned<T, Storage, H, I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized, Storage, H, I: Index> Ord for Interned<T, Storage, H, I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T: ?Sized, Storage, H, I: Index> Hash for Interned<T, Storage, H, I> {
    fn hash<G>(&self, state: &mut G)
    where
        G: Hasher,
    {
        self.id.hash(state);
    }
}

#[cfg(feature = "get-size2")]
impl<T: ?Sized, Storage, H, I> GetSize for Interned<T, Storage, H, I>
where
    I: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.id.get_heap_size_with_tracker(tracker)
    }
}

#[cfg(feature = "raw")]
impl<T: ?Sized, Storage, H, I> Interned<T, Storage, H, I> {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`Arena::intern()`] API to intern a value, unless you really know what
    /// you're doing.
    pub fn from_id(id: I) -> Self {
        Self::new(id)
    }
}

#[cfg(feature = "raw")]
impl<T: ?Sized, Storage, H, I: Index> Interned<T, Storage, H, I> {
    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`Arena::lookup()`] and [`Arena::lookup_ref()`] APIs, unless you really
    /// know what you're doing.
    pub fn id(&self) -> I {
        self.id
    }
}

impl<T: ?Sized, Storage, H, I> Interned<T, Storage, H, I> {
    pub(crate) fn new(id: I) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage, H, I: Index> Interned<T, Storage, H, I> {
    pub(crate) fn id_(&self) -> I {
        self.id
    }
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage, H, I> Serialize for Interned<T, Storage, H, I>
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
impl<'de, T: ?Sized, Storage, H, I> Deserialize<'de> for Interned<T, Storage, H, I>
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

#[cfg(feature = "postcard-schema")]
impl<T: ?Sized, Storage, H, I> Schema for Interned<T, Storage, H, I>
where
    I: Schema,
{
    const SCHEMA: &'static NamedType = &NamedType {
        name: "Interned",
        ty: &DataModelType::NewtypeStruct(I::SCHEMA),
    };
}

/// Interning arena for values of type `T`, storing them with the given
/// `Storage` type (that needs to be [`Sized`]).
///
/// For [`Sized`] values, `Storage = T` is a good default that incurs no
/// overhead. For non-[`Sized`] values such as [`dyn
/// Trait`](https://doc.rust-lang.org/stable/std/keyword.dyn.html), you need to
/// specify a [`Sized`] storage type, such as `Box<dyn Trait>`.
pub struct Arena<T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    #[cfg(not(feature = "sync"))]
    vec: Vec<Storage>,
    #[cfg(feature = "sync")]
    vec: AppendVec<Storage>,
    #[cfg(not(feature = "sync"))]
    map: HashTable<I>,
    #[cfg(feature = "sync")]
    map: DashTable<I>,
    hasher: H,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
    _phantom: PhantomData<fn() -> *const T>,
}

impl<T: ?Sized, Storage, H, I> Clone for Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Clone,
    H: Default + BuildHasher,
    I: Index,
{
    fn clone(&self) -> Self {
        let iter = self.vec.iter();
        let mut arena = Self::with_capacity(iter.len());
        for value in iter {
            arena.push(value.clone());
        }
        arena
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    H: Default,
    I: Index,
{
    /// Creates a new arena with pre-allocated space to store at least `len`
    /// values of type `T`.
    ///
    /// # Panics
    ///
    /// This function panics if the capacity exceeds the maximum allocation size
    /// for an arena indexed by `I`. Use
    /// [`try_with_capacity`](Self::try_with_capacity) for a fallible API.
    pub fn with_capacity(len: usize) -> Self {
        match Self::try_with_capacity(len) {
            Some(x) => x,
            None => panic!(
                "Cannot allocate an Arena with capacity {len} with indices up to {}",
                I::MAX.to_usize()
            ),
        }
    }

    /// Creates a new arena with pre-allocated space to store at least `len`
    /// values of type `T`.
    ///
    /// This returns [`None`] if the capacity exceeds the maximum allocation
    /// size for an arena indexed by `I`.
    pub fn try_with_capacity(len: usize) -> Option<Self> {
        if len != 0 && len - 1 > I::MAX.to_usize() {
            return None;
        }
        Some(Self {
            #[cfg(not(feature = "sync"))]
            vec: Vec::with_capacity(len),
            #[cfg(feature = "sync")]
            vec: AppendVec::try_with_capacity(len)?,
            #[cfg(not(feature = "sync"))]
            map: HashTable::with_capacity(len),
            #[cfg(feature = "sync")]
            map: DashTable::with_capacity(len),
            hasher: H::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
            _phantom: PhantomData,
        })
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I> {
    /// Returns the number of values in this arena.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot as viewed by this thread, and the result may change if other
    /// threads are inserting values.
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    /// Checks if this arena is empty.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot as viewed by this thread, and the result may change if other
    /// threads are inserting values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a snapshot of this arena.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot as viewed by this thread.
    ///
    /// Snapshots can be diffed into a [`SnapshotDiff`], which is useful to
    /// serialize an arena incrementally as values are added to it.
    pub fn snapshot(&self) -> Snapshot<'_, T, Storage, H, I> {
        Snapshot {
            arena: self,
            mark: self.len(),
        }
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    Storage: Borrow<T>,
    I: Index,
{
    /// Returns an iterator over all items in this arena, in indexing order.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot. Once this iterator has been created, for performance reasons
    /// it will not iterate over items added afterwards, even on the same
    /// thread.
    #[cfg(feature = "raw")]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        self.iter_()
    }

    fn iter_(&self) -> impl ExactSizeIterator<Item = &T> {
        self.vec.iter().map(|x| x.borrow())
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
    H: BuildHasher,
    I: Index,
{
    /// Returns the given value's [`Interned`] handle if it is already interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// See also [`find_mut()`](Self::find_mut), which is more efficient if you
    /// hold a mutable reference to this arena as it avoids acquiring locks.
    pub fn find(&self, value: &T) -> Option<Interned<T, Storage, H, I>> {
        let hash = self.hasher.hash_one(value);
        self.map
            .find(hash, |&i| self.vec[i.to_usize()].borrow() == value)
            .map(|id| Interned::new(*id))
    }

    /// Returns the given value's [`Interned`] handle if it is already interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// Contrary to [`find()`](Self::find), no locks are held internally because
    /// this function already takes an exclusive mutable reference to this
    /// arena.
    pub fn find_mut(&mut self, value: &T) -> Option<Interned<T, Storage, H, I>> {
        let hash = self.hasher.hash_one(value);
        #[cfg(not(feature = "sync"))]
        return self
            .map
            .find(hash, |&i| self.vec[i.to_usize()].borrow() == value)
            .map(|id| Interned::new(*id));
        #[cfg(feature = "sync")]
        return self
            .map
            .find_mut(hash, |&i| self.vec[i.to_usize()].borrow() == value)
            .map(|id| Interned::new(*id));
    }
}

#[cfg(feature = "raw")]
impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
    H: BuildHasher,
    I: Index,
{
    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full (based on the index type `I`).
    /// Use [`try_push_mut`](Self::try_push_mut) for a fallible API.
    pub fn push_mut(&mut self, value: Storage) -> I {
        self.push(value)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// This returns [`None`] if the arena is full (based on the index type
    /// `I`).
    pub fn try_push_mut(&mut self, value: Storage) -> Option<I> {
        self.try_push(value)
    }
}

impl<T: ?Sized, Storage, H, I> Default for Arena<T, Storage, H, I>
where
    H: Default,
{
    fn default() -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            vec: Vec::new(),
            #[cfg(feature = "sync")]
            vec: AppendVec::new(),
            #[cfg(not(feature = "sync"))]
            map: HashTable::new(),
            #[cfg(feature = "sync")]
            map: DashTable::new(),
            hasher: H::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage, H, I> Debug for Arena<T, Storage, H, I>
where
    T: Debug,
    Storage: Borrow<T>,
    I: Index,
{
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        fmt.debug_list().entries(self.iter_()).finish()
    }
}

impl<T: ?Sized, Storage, H, I> PartialEq for Arena<T, Storage, H, I>
where
    T: Eq,
    Storage: Borrow<T>,
    I: Index,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter_().eq(other.iter_())
    }
}

impl<T: ?Sized, Storage, H, I> Eq for Arena<T, Storage, H, I>
where
    T: Eq,
    Storage: Borrow<T>,
    I: Index,
{
}

#[cfg(feature = "get-size2")]
impl<T: ?Sized, Storage, H, I> GetSize for Arena<T, Storage, H, I>
where
    Storage: GetSize,
    I: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let (size_vec, tracker) = GetSize::get_heap_size_with_tracker(&self.vec, tracker);
        let (size_map, tracker) = GetSize::get_heap_size_with_tracker(&self.map, tracker);
        (size_vec + size_map, tracker)
    }
}

#[cfg(all(feature = "debug", feature = "std"))]
impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    Storage: GetSize,
{
    /// Prints a summary of the storage used by this arena to stdout.
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let len = self.vec.len();
        let references = self.references();
        let estimated_bytes = self.get_size();
        println!(
            "{}[{:.02}%] {} interner: {} objects | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            prefix,
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            title,
            len,
            estimated_bytes,
            estimated_bytes as f64 / len as f64,
            references,
            references as f64 / len as f64,
        );
    }
}

#[cfg(feature = "debug")]
impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I> {
    /// Returns the total number of references to items in this arena.
    ///
    /// The underlying counter is incremented each time a value is interned,
    /// whether it was already previously in the arena or not.
    pub fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
    H: BuildHasher,
    I: Index,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    ///
    /// See also [`intern_mut()`](Self::intern_mut), which is more efficient if
    /// you hold a mutable reference to this arena as it avoids acquiring locks.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full (based on the index type `I`).
    /// Use [`try_intern()`](Self::try_intern) for a fallible API.
    #[cfg(feature = "sync")]
    pub fn intern(&self, value: impl Borrow<T> + Into<Storage>) -> Interned<T, Storage, H, I> {
        match self.try_intern(value) {
            Some(x) => x,
            None => panic!("Cannot intern in a full Arena"),
        }
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    ///
    /// This returns [`None`] if the arena is full (based on the index type
    /// `I`).
    ///
    /// See also [`try_intern_mut()`](Self::try_intern_mut), which is more
    /// efficient if you hold a mutable reference to this arena as it avoids
    /// acquiring locks.
    #[cfg(feature = "sync")]
    pub fn try_intern(
        &self,
        value: impl Borrow<T> + Into<Storage>,
    ) -> Option<Interned<T, Storage, H, I>> {
        let hash = self.hasher.hash_one(value.borrow());
        let entry = self.map.entry(
            hash,
            |&i| self.vec[i.to_usize()].borrow() == value.borrow(),
            |&i| self.hasher.hash_one(self.vec[i.to_usize()].borrow()),
        );
        let id = match entry {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let x: Storage = value.into();
                let id = self.vec.try_push(x, I::MAX.to_usize())?;
                *e.insert(I::from_usize(id)).get()
            }
        };

        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);
        Some(Interned::new(id))
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    ///
    /// Contrary to [`intern()`](Self::intern), no locks are held internally
    /// because this function already takes an exclusive mutable reference to
    /// this arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full (based on the index type `I`).
    /// Use [`try_intern_mut()`](Self::try_intern_mut) for a fallible API.
    pub fn intern_mut(
        &mut self,
        value: impl Borrow<T> + Into<Storage>,
    ) -> Interned<T, Storage, H, I> {
        match self.try_intern_mut(value) {
            Some(x) => x,
            None => panic!("Cannot intern in a full Arena"),
        }
    }

    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    ///
    /// This returns [`None`] if the arena is full (based on the index type
    /// `I`).
    ///
    /// Contrary to [`try_intern()`](Self::try_intern), no locks are held
    /// internally because this function already takes an exclusive mutable
    /// reference to this arena.
    pub fn try_intern_mut(
        &mut self,
        value: impl Borrow<T> + Into<Storage>,
    ) -> Option<Interned<T, Storage, H, I>> {
        let hash = self.hasher.hash_one(value.borrow());
        #[cfg(not(feature = "sync"))]
        let id = {
            let entry = self.map.entry(
                hash,
                |&i| self.vec[i.to_usize()].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i.to_usize()].borrow()),
            );
            match entry {
                Entry::Occupied(e) => *e.get(),
                Entry::Vacant(e) => {
                    let id = self.vec.len();
                    if id > I::MAX.to_usize() {
                        return None;
                    }
                    let x: Storage = value.into();
                    self.vec.push(x);
                    *e.insert(I::from_usize(id)).get()
                }
            }
        };
        #[cfg(feature = "sync")]
        let id = {
            let entry = self.map.entry_mut(
                hash,
                |&i| self.vec[i.to_usize()].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i.to_usize()].borrow()),
            );
            match entry {
                MutEntry::Occupied(e) => *e.get(),
                MutEntry::Vacant(e) => {
                    let x: Storage = value.into();
                    let id = self.vec.try_push_mut(x, I::MAX.to_usize())?;
                    *e.insert(I::from_usize(id)).get()
                }
            }
        };

        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }
        Some(Interned::new(id))
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// # Panics
    ///
    /// This function panics if the arena is full (based on the index type `I`).
    /// Use [`try_push()`](Self::try_push) for a fallible API.
    pub(crate) fn push(&mut self, value: Storage) -> I {
        match self.try_push(value) {
            Some(x) => x,
            None => panic!("Cannot intern in a full Arena"),
        }
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    ///
    /// This returns [`None`] if the arena is full (based on the index type
    /// `I`).
    pub(crate) fn try_push(&mut self, value: Storage) -> Option<I> {
        let hash = self.hasher.hash_one(value.borrow());

        #[cfg(not(feature = "sync"))]
        let id = {
            let id = self.vec.len();
            if id > I::MAX.to_usize() {
                return None;
            }
            self.vec.push(value);
            let id = I::from_usize(id);
            self.map.insert_unique(hash, id, |&i| {
                self.hasher.hash_one(self.vec[i.to_usize()].borrow())
            });
            id
        };
        #[cfg(feature = "sync")]
        let id = {
            let id = self.vec.try_push_mut(value, I::MAX.to_usize())?;
            let id = I::from_usize(id);
            self.map.insert_unique_mut(hash, id, |&i| {
                self.hasher.hash_one(self.vec[i.to_usize()].borrow())
            });
            id
        };

        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }
        Some(id)
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    Storage: Clone,
    I: Index,
{
    /// Retrieves the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup_ref()`](Self::lookup_ref) if you only need a
    /// reference.
    ///
    /// # Panics
    ///
    /// This function panics if the interned handle is out of bounds for this
    /// arena, for example because the arena was truncated, or because the
    /// interned handle was deserialized from untrusted data. Use
    /// [`try_lookup`](Self::try_lookup) for a fallible API.
    pub fn lookup(&self, interned: Interned<T, Storage, H, I>) -> Storage {
        self.vec[interned.id.to_usize()].clone()
    }

    /// Retrieves the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned.
    ///
    /// This returns [`None`] if the interned handle is out of bounds for this
    /// arena, for example because the arena was truncated, or because the
    /// interned handle was deserialized from untrusted data.
    ///
    /// See also [`try_lookup_ref()`](Self::try_lookup_ref) if you only need a
    /// reference.
    pub fn try_lookup(&self, interned: Interned<T, Storage, H, I>) -> Option<Storage> {
        let i = interned.id.to_usize();
        #[cfg(not(feature = "sync"))]
        let value = self.vec.get(i)?;
        #[cfg(feature = "sync")]
        let value = self.vec.try_index(i)?;
        Some(value.clone())
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    Storage: Borrow<T>,
    I: Index,
{
    /// Retrieves a reference to the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup()`](Self::lookup) if you need an owned value.
    ///
    /// # Panics
    ///
    /// This function panics if the interned handle is out of bounds for this
    /// arena, for example because the arena was truncated, or because the
    /// interned handle was deserialized from untrusted data. Use
    /// [`try_lookup_ref`](Self::try_lookup_ref) for a fallible API.
    pub fn lookup_ref(&self, interned: Interned<T, Storage, H, I>) -> &T {
        self.vec[interned.id.to_usize()].borrow()
    }

    /// Retrieves a reference to the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned.
    ///
    /// This returns [`None`] if the interned handle is out of bounds for this
    /// arena, for example because the arena was truncated, or because the
    /// interned handle was deserialized from untrusted data.
    ///
    /// See also [`try_lookup()`](Self::try_lookup) if you need an owned value.
    pub fn try_lookup_ref(&self, interned: Interned<T, Storage, H, I>) -> Option<&T> {
        let i = interned.id.to_usize();
        #[cfg(not(feature = "sync"))]
        let value = self.vec.get(i)?;
        #[cfg(feature = "sync")]
        let value = self.vec.try_index(i)?;
        Some(value.borrow())
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
    H: BuildHasher,
    I: Index,
{
    /// Truncates this interner back to the given snapshot mark.
    ///
    /// This is a no-op if the snapshot mark is larger than this interner (for
    /// example because the interner was already truncated to a smaller snapshot
    /// in the meantime).
    pub fn truncate(&mut self, snapshot: SnapshotMark<T, Storage, H, I>) {
        if self.len() <= snapshot.mark {
            return;
        }

        for value in self.vec.iter().skip(snapshot.mark) {
            let hash = self.hasher.hash_one(value.borrow());
            #[cfg(not(feature = "sync"))]
            if let Entry::Occupied(entry) = self.map.entry(
                hash,
                |&i| self.vec[i.to_usize()].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i.to_usize()].borrow()),
            ) {
                entry.remove();
            }
            #[cfg(feature = "sync")]
            if let MutEntry::Occupied(entry) = self.map.entry_mut(
                hash,
                |&i| self.vec[i.to_usize()].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i.to_usize()].borrow()),
            ) {
                entry.remove();
            }
        }

        self.vec.truncate(snapshot.mark);
    }
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage, H, I> Serialize for Arena<T, Storage, H, I>
where
    T: Serialize,
    Storage: Borrow<T>,
    I: Index,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter_())
    }
}

#[cfg(feature = "postcard-schema")]
impl<T: ?Sized, Storage, H, I> Schema for Arena<T, Storage, H, I>
where
    T: Schema,
{
    const SCHEMA: &'static NamedType = &NamedType {
        name: "Arena",
        ty: &DataModelType::Seq(T::SCHEMA),
    };
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage, H, I> Deserialize<'de> for Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArenaVisitor::new())
    }
}

#[cfg(feature = "serde")]
struct ArenaVisitor<T: ?Sized, Storage, H, I> {
    #[expect(clippy::type_complexity)]
    _phantom: PhantomData<fn() -> Arena<T, Storage, H, I>>,
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage, H, I> ArenaVisitor<T, Storage, H, I> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage, H, I> Visitor<'de> for ArenaVisitor<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index,
{
    type Value = Arena<T, Storage, H, I>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a sequence of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut arena = match seq.size_hint() {
            None => Arena::default(),
            Some(size_hint) => Arena::try_with_capacity(size_hint).ok_or_else(|| {
                Error::custom(format!(
                    "Cannot create an Arena with capacity for {size_hint} values"
                ))
            })?,
        };

        while let Some(t) = seq.next_element()? {
            arena.try_push(t).ok_or(Error::custom(
                "Arena is full, cannot push more values to it",
            ))?;
        }

        Ok(arena)
    }
}

/// A mark indicates the position of a [`Snapshot`] in an [`Arena`].
///
/// This allows creating a difference between two snapshots, via
/// [`Snapshot::diff()`].
#[expect(clippy::type_complexity)]
pub struct SnapshotMark<T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    mark: usize,
    _phantom: PhantomData<fn() -> Arena<T, Storage, H, I>>,
}

impl<T: ?Sized, Storage, H, I> Default for SnapshotMark<T, Storage, H, I> {
    fn default() -> Self {
        Self {
            mark: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage, H, I> Clone for SnapshotMark<T, Storage, H, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized, Storage, H, I> Copy for SnapshotMark<T, Storage, H, I> {}

/// Snapshot of an [`Arena`].
///
/// This struct is created by the [`snapshot()`](Arena::snapshot) function on
/// [`Arena`].
pub struct Snapshot<'a, T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    #[cfg_attr(not(feature = "serde"), expect(dead_code))]
    arena: &'a Arena<T, Storage, H, I>,
    mark: usize,
}

impl<T: ?Sized, Storage, H, I> Snapshot<'_, T, Storage, H, I> {
    /// Returns the position of this snapshot in the arena.
    pub fn mark(&self) -> SnapshotMark<T, Storage, H, I> {
        SnapshotMark {
            mark: self.mark,
            _phantom: PhantomData,
        }
    }

    /// Returns the difference between this snapshot and a previous mark.
    #[cfg(feature = "serde")]
    pub fn diff(
        &self,
        start: SnapshotMark<T, Storage, H, I>,
    ) -> SnapshotDiff<'_, T, Storage, H, I> {
        SnapshotDiff {
            arena: self.arena,
            start: start.mark,
            end: self.mark,
        }
    }
}

/// Difference between two snapshots of an [`Arena`].
///
/// This is useful to serialize an arena incrementally as more values are added
/// to it.
#[cfg(feature = "serde")]
pub struct SnapshotDiff<'a, T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    arena: &'a Arena<T, Storage, H, I>,
    start: usize,
    end: usize,
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage, H, I> SnapshotDiff<'_, T, Storage, H, I>
where
    Storage: Borrow<T>,
{
    fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        self.arena
            .vec
            .iter()
            .take(self.end)
            .skip(self.start)
            .map(|x| x.borrow())
    }
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage, H, I> Serialize for SnapshotDiff<'_, T, Storage, H, I>
where
    T: Serialize,
    Storage: Borrow<T>,
    I: Index,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

#[cfg(all(feature = "serde", feature = "postcard-schema"))]
impl<T: ?Sized, Storage, H, I> Schema for SnapshotDiff<'_, T, Storage, H, I>
where
    T: Schema,
{
    const SCHEMA: &'static NamedType = &NamedType {
        name: "SnapshotDiff",
        ty: &DataModelType::Seq(T::SCHEMA),
    };
}

/// Wrapper to extend an [`Arena`] incrementally, typically by deserializing a
/// sequence of serialized [`SnapshotDiff`].
#[cfg(feature = "serde")]
pub struct ExtendArena<'a, T: ?Sized, Storage = T, H = DefaultBuildHasher, I = u32> {
    arena: &'a mut Arena<T, Storage, H, I>,
}

#[cfg(feature = "serde")]
impl<'a, T: ?Sized, Storage, H, I> ExtendArena<'a, T, Storage, H, I> {
    /// Wraps the given arena to deserialize into it.
    pub fn new(arena: &'a mut Arena<T, Storage, H, I>) -> Self {
        Self { arena }
    }
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage, H, I> DeserializeSeed<'de> for ExtendArena<'_, T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ExtendArenaVisitor { arena: self.arena })
    }
}

#[cfg(feature = "serde")]
struct ExtendArenaVisitor<'a, T: ?Sized, Storage, H, I> {
    arena: &'a mut Arena<T, Storage, H, I>,
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage, H, I> Visitor<'de> for ExtendArenaVisitor<'_, T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
    H: Default + BuildHasher,
    I: Index,
{
    type Value = ();

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a sequence of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(t) = seq.next_element()? {
            self.arena.try_push(t).ok_or(Error::custom(
                "Arena is full, cannot push more values to it",
            ))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::borrow::Cow;
    #[cfg(feature = "sync")]
    use std::thread;

    #[cfg(feature = "postcard-schema")]
    #[test]
    fn test_schemas() {
        assert_eq!(
            Interned::<()>::SCHEMA,
            &NamedType {
                name: "Interned",
                ty: &DataModelType::NewtypeStruct(&NamedType {
                    name: "u32",
                    ty: &DataModelType::U32,
                }),
            }
        );
        assert_eq!(
            Arena::<()>::SCHEMA,
            &NamedType {
                name: "Arena",
                ty: &DataModelType::Seq(&NamedType {
                    name: "()",
                    ty: &DataModelType::Unit,
                }),
            }
        );
        #[cfg(feature = "serde")]
        assert_eq!(
            SnapshotDiff::<()>::SCHEMA,
            &NamedType {
                name: "SnapshotDiff",
                ty: &DataModelType::Seq(&NamedType {
                    name: "()",
                    ty: &DataModelType::Unit,
                }),
            }
        );
    }

    #[test]
    fn test_default_build_hasher() {
        let build_hasher = DefaultBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher.hash_one(i);
            let hash2 = build_hasher.hash_one(i);
            assert_eq!(hash1, hash2);
        }

        let hash1 = build_hasher.hash_one("Hello world");
        let hash2 = build_hasher.hash_one("Hello world");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hashbrown_build_hasher() {
        let build_hasher = HashbrownBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher.hash_one(i);
            let hash2 = build_hasher.hash_one(i);
            assert_eq!(hash1, hash2);
        }

        let hash1 = build_hasher.hash_one("Hello world");
        let hash2 = build_hasher.hash_one("Hello world");
        assert_eq!(hash1, hash2);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_std_build_hasher() {
        let build_hasher = StdBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher.hash_one(i);
            let hash2 = build_hasher.hash_one(i);
            assert_eq!(hash1, hash2);
        }

        let hash1 = build_hasher.hash_one("Hello world");
        let hash2 = build_hasher.hash_one("Hello world");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_default_build_hasher_is_randomized() {
        let build_hasher1 = DefaultBuildHasher::default();
        let build_hasher2 = DefaultBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher1.hash_one(i);
            let hash2 = build_hasher2.hash_one(i);
            if hash1 != hash2 {
                return;
            }
        }
        panic!("All hashes match");
    }

    #[test]
    fn test_hashbrown_build_hasher_is_randomized() {
        let build_hasher1 = HashbrownBuildHasher::default();
        let build_hasher2 = HashbrownBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher1.hash_one(i);
            let hash2 = build_hasher2.hash_one(i);
            if hash1 != hash2 {
                return;
            }
        }
        panic!("All hashes match");
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_std_build_hasher_is_randomized() {
        let build_hasher1 = StdBuildHasher::default();
        let build_hasher2 = StdBuildHasher::default();

        for i in 0..42 {
            let hash1 = build_hasher1.hash_one(i);
            let hash2 = build_hasher2.hash_one(i);
            if hash1 != hash2 {
                return;
            }
        }
        panic!("All hashes match");
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup() {
        let arena: Arena<usize> = Arena::default();
        for i in 0..100 {
            assert_eq!(arena.intern(2 * i).id.to_usize(), i);
        }
        for i in 0..100 {
            assert_eq!(
                *arena.lookup_ref(Interned::new(Index::from_usize(i))),
                2 * i
            );
            assert_eq!(arena.lookup(Interned::new(Index::from_usize(i))), 2 * i);
        }
    }

    #[test]
    fn test_intern_mut_lookup() {
        let mut arena: Arena<usize> = Arena::default();
        for i in 0..100 {
            assert_eq!(arena.intern_mut(2 * i).id.to_usize(), i);
        }
        for i in 0..100 {
            assert_eq!(
                *arena.lookup_ref(Interned::new(Index::from_usize(i))),
                2 * i
            );
            assert_eq!(arena.lookup(Interned::new(Index::from_usize(i))), 2 * i);
        }
    }

    #[cfg(all(feature = "raw", not(miri)))]
    const NUM_ITERS: usize = 100;
    #[cfg(all(feature = "raw", miri))]
    const NUM_ITERS: usize = 20;
    #[cfg(all(feature = "raw", not(miri)))]
    const NUM_VALUES: usize = 50;
    #[cfg(all(feature = "raw", miri))]
    const NUM_VALUES: usize = 10;

    #[cfg(feature = "raw")]
    #[test]
    fn test_push_mut_same_value_works() {
        let mut arena: Arena<Box<usize>> = Arena::default();
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                assert_eq!(
                    arena.push_mut(Box::new(42 + j)).to_usize(),
                    i * NUM_VALUES + j
                );
                let id = arena.intern_mut(Box::new(42 + j)).id;
                let id = id.to_usize();
                assert_eq!(id % NUM_VALUES, j);
                assert!(id / NUM_VALUES <= i);
            }
        }
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                assert_eq!(
                    **arena.lookup_ref(Interned::new(Index::from_usize(i * NUM_VALUES + j))),
                    42 + j
                );
                assert_eq!(
                    *arena.lookup(Interned::new(Index::from_usize(i * NUM_VALUES + j))),
                    42 + j
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
        let arena: Arena<usize, Box<usize>> = Arena::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.len();
                        if len > 0 {
                            let last = len - 1;
                            assert_eq!(
                                *arena.lookup_ref(Interned::new(Index::from_usize(last))),
                                last
                            );
                            if len == NUM_ITEMS {
                                break;
                            }
                        }
                    }
                });
            }
            s.spawn(|| {
                for j in 0..NUM_ITEMS {
                    assert_eq!(arena.intern(j).id.to_usize(), j);
                }
            });
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_writes() {
        let arena: Arena<usize, Box<usize>> = Arena::default();
        thread::scope(|s| {
            s.spawn(|| {
                loop {
                    let len = arena.len();
                    if len > 0 {
                        let last = len - 1;
                        assert_eq!(
                            *arena.lookup_ref(Interned::new(Index::from_usize(last))),
                            last
                        );
                        if len == NUM_ITEMS {
                            break;
                        }
                    }
                }
            });
            for _ in 0..NUM_WRITERS {
                s.spawn(|| {
                    for j in 0..NUM_ITEMS {
                        assert_eq!(arena.intern(j).id.to_usize(), j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_readwrites() {
        let arena: Arena<usize, Box<usize>> = Arena::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.len();
                        if len > 0 {
                            let last = len - 1;
                            assert_eq!(
                                *arena.lookup_ref(Interned::new(Index::from_usize(last))),
                                last
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
                    for j in 0..NUM_ITEMS {
                        assert_eq!(arena.intern(j).id.to_usize(), j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_boxed_str_interner() {
        let arena: Arena<str, Box<str>> = Arena::default();

        let key: &str = "Hello";
        assert_eq!(arena.intern(key).id.to_usize(), 0);

        let key: String = "world".into();
        assert_eq!(arena.intern(key).id.to_usize(), 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(arena.intern(key).id.to_usize(), 0);

        let key: Box<str> = "world".into();
        assert_eq!(arena.intern(key).id.to_usize(), 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(arena.intern(key).id.to_usize(), 2);
    }

    #[test]
    fn test_boxed_str_interner_mut() {
        let mut arena: Arena<str, Box<str>> = Arena::default();

        let key: &str = "Hello";
        assert_eq!(arena.intern_mut(key).id.to_usize(), 0);

        let key: String = "world".into();
        assert_eq!(arena.intern_mut(key).id.to_usize(), 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(arena.intern_mut(key).id.to_usize(), 0);

        let key: Box<str> = "world".into();
        assert_eq!(arena.intern_mut(key).id.to_usize(), 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(arena.intern_mut(key).id.to_usize(), 2);
    }

    #[test]
    fn test_truncate() {
        let mut arena: Arena<usize, Box<usize>> = Arena::default();

        let a = arena.intern_mut(0);
        let b = arena.intern_mut(1);
        let c = arena.intern_mut(2);
        let d = arena.intern_mut(3);
        let snapshot = arena.snapshot().mark();

        arena.intern_mut(4);
        arena.intern_mut(5);

        arena.truncate(snapshot);
        assert_eq!(arena.len(), 4);
        assert_eq!(arena.lookup_ref(a), &0);
        assert_eq!(arena.lookup_ref(b), &1);
        assert_eq!(arena.lookup_ref(c), &2);
        assert_eq!(arena.lookup_ref(d), &3);

        let f = arena.intern_mut(6);
        let g = arena.intern_mut(7);

        assert_eq!(arena.len(), 6);
        assert_eq!(arena.lookup_ref(a), &0);
        assert_eq!(arena.lookup_ref(b), &1);
        assert_eq!(arena.lookup_ref(c), &2);
        assert_eq!(arena.lookup_ref(d), &3);
        assert_eq!(arena.lookup_ref(f), &6);
        assert_eq!(arena.lookup_ref(g), &7);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_postcard() {
        let mut arena: Arena<u32> = Arena::default();

        let a = arena.intern_mut(0);
        let b = arena.intern_mut(1);
        let c = arena.intern_mut(22);
        let d = arena.intern_mut(333);
        let e = arena.intern_mut(4444);
        let f = arena.intern_mut(55555);

        assert_eq!(arena.len(), 6);

        let serialized_arena = postcard::to_stdvec(&arena).expect("Failed to serialize arena");
        assert_eq!(
            serialized_arena,
            vec![6, 0, 1, 22, 205, 2, 220, 34, 131, 178, 3]
        );
        let new_arena: Arena<u32> =
            postcard::from_bytes(&serialized_arena).expect("Failed to deserialize arena");
        assert_eq!(new_arena, arena);

        assert_eq!(new_arena.len(), 6);

        let serialized_handles =
            postcard::to_stdvec(&[a, b, c, d, e, f]).expect("Failed to serialize interned handles");
        assert_eq!(serialized_handles, vec![0, 1, 2, 3, 4, 5]);
        let new_handles: [Interned<u32>; 6] = postcard::from_bytes(&serialized_handles)
            .expect("Failed to deserialize interned handles");
        assert_eq!(new_handles, [a, b, c, d, e, f]);

        assert_eq!(new_arena.lookup(a), 0);
        assert_eq!(new_arena.lookup(b), 1);
        assert_eq!(new_arena.lookup(c), 22);
        assert_eq!(new_arena.lookup(d), 333);
        assert_eq!(new_arena.lookup(e), 4444);
        assert_eq!(new_arena.lookup(f), 55555);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_json() {
        let mut arena: Arena<u32> = Arena::default();

        let a = arena.intern_mut(0);
        let b = arena.intern_mut(1);
        let c = arena.intern_mut(22);
        let d = arena.intern_mut(333);
        let e = arena.intern_mut(4444);
        let f = arena.intern_mut(55555);

        assert_eq!(arena.len(), 6);

        let serialized_arena = serde_json::to_string(&arena).expect("Failed to serialize arena");
        assert_eq!(serialized_arena, "[0,1,22,333,4444,55555]");
        let new_arena: Arena<u32> =
            serde_json::from_str(&serialized_arena).expect("Failed to deserialize arena");
        assert_eq!(new_arena, arena);

        assert_eq!(new_arena.len(), 6);

        let serialized_handles = serde_json::to_string(&[a, b, c, d, e, f])
            .expect("Failed to serialize interned handles");
        assert_eq!(serialized_handles, "[0,1,2,3,4,5]");
        let new_handles: [Interned<u32>; 6] = serde_json::from_str(&serialized_handles)
            .expect("Failed to deserialize interned handles");
        assert_eq!(new_handles, [a, b, c, d, e, f]);
    }
}
