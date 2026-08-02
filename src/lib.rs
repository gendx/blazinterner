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
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "delta")]
mod delta;
mod mapping;
mod slice;
mod str;

#[cfg(feature = "sync")]
use appendvec::AppendVec;
#[cfg(feature = "sync")]
use dashtable::DashTable;
#[cfg(feature = "delta")]
pub use delta::{Accumulator, DeltaEncoding};
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use hashbrown::DefaultHashBuilder;
#[cfg(not(feature = "sync"))]
use hashbrown::HashTable;
pub use mapping::{ForwardMapping, Mapping, ReverseMapping};
#[cfg(feature = "retain")]
pub use mapping::{RetainBuilder, RetainSliceBuilder, RetainStrBuilder};
#[cfg(feature = "serde")]
use serde::de::{SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use slice::CopyRangeU32;
pub use slice::{ArenaSlice, InternedSlice};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
#[cfg(feature = "get-size2")]
use std::mem::size_of;
#[cfg(feature = "debug")]
use std::sync::atomic::{self, AtomicUsize};
pub use str::{ArenaStr, InternedStr};

/// A handle to an interned value in an [`Arena`].
///
/// This is generic over the logical value type `T` as well as its `Storage`
/// type, that needs to be [`Sized`]. For [`Sized`] values, `Storage = T` is a
/// good default that incurs no overhead. For non-[`Sized`] values such as
/// [`str`](prim@str), you need to specify a [`Sized`] storage type, such as
/// `Box<T>`.
pub struct Interned<T: ?Sized, Storage = T> {
    id: u32,
    _phantom: PhantomData<fn() -> (*const T, *const Storage)>,
}

impl<T: ?Sized, Storage> Default for Interned<T, Storage> {
    fn default() -> Self {
        Self::new(u32::MAX)
    }
}

impl<T: ?Sized, Storage> Debug for Interned<T, Storage> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T: ?Sized, Storage> Clone for Interned<T, Storage> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized, Storage> Copy for Interned<T, Storage> {}

impl<T: ?Sized, Storage> PartialEq for Interned<T, Storage> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T: ?Sized, Storage> Eq for Interned<T, Storage> {}

impl<T: ?Sized, Storage> PartialOrd for Interned<T, Storage> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized, Storage> Ord for Interned<T, Storage> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T: ?Sized, Storage> Hash for Interned<T, Storage> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

#[cfg(feature = "get-size2")]
impl<T: ?Sized, Storage> GetSize for Interned<T, Storage> {
    // There is nothing on the heap, so the default implementation works out of the
    // box.
}

#[cfg(feature = "raw")]
impl<T: ?Sized, Storage> Interned<T, Storage> {
    /// Creates an interned value for the given index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`Arena::intern()`] API to intern a value, unless you really know what
    /// you're doing.
    pub fn from_id(id: u32) -> Self {
        Self::new(id)
    }

    /// Obtains the underlying interning index.
    ///
    /// This is a low-level function. You should instead use the
    /// [`Arena::lookup()`] and [`Arena::lookup_ref()`] APIs, unless you really
    /// know what you're doing.
    pub fn id(&self) -> u32 {
        self.id
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage> {
    pub(crate) fn new(id: u32) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn id_(&self) -> u32 {
        self.id
    }
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage> Serialize for Interned<T, Storage> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.id)
    }
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage> Deserialize<'de> for Interned<T, Storage> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = u32::deserialize(deserializer)?;
        Ok(Self {
            id,
            _phantom: PhantomData,
        })
    }
}

/// Interning arena for values of type `T`, storing them with the given
/// `Storage` type (that needs to be [`Sized`]).
///
/// For [`Sized`] values, `Storage = T` is a good default that incurs no
/// overhead. For non-[`Sized`] values such as [`dyn
/// Trait`](https://doc.rust-lang.org/stable/std/keyword.dyn.html), you need to
/// specify a [`Sized`] storage type, such as `Box<dyn Trait>`.
pub struct Arena<T: ?Sized, Storage = T> {
    #[cfg(not(feature = "sync"))]
    vec: Vec<Storage>,
    #[cfg(feature = "sync")]
    vec: AppendVec<Storage>,
    #[cfg(not(feature = "sync"))]
    map: HashTable<u32>,
    #[cfg(feature = "sync")]
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
    _phantom: PhantomData<fn() -> *const T>,
}

impl<T: ?Sized, Storage> Clone for Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Clone,
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

impl<T: ?Sized, Storage> Arena<T, Storage> {
    /// Creates a new arena with pre-allocated space to store at least `len`
    /// values of type `T`.
    pub fn with_capacity(len: usize) -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            vec: Vec::with_capacity(len),
            #[cfg(feature = "sync")]
            vec: AppendVec::with_capacity(len),
            #[cfg(not(feature = "sync"))]
            map: HashTable::with_capacity(len),
            #[cfg(feature = "sync")]
            map: DashTable::with_capacity(len),
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }

    /// Returns the number of values in this arena.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot as viewed by this thread, and the result may change if
    /// other threads are inserting values.
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    /// Checks if this arena is empty.
    ///
    /// Note that because [`Arena`] is a concurrent data structure, this is only
    /// a snapshot as viewed by this thread, and the result may change if
    /// other threads are inserting values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    Storage: Borrow<T>,
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

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    /// Returns the given value's [`Interned`] handle if it is already interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// See also [`find_mut()`](Self::find_mut), which is more efficient if you
    /// hold a mutable reference to this arena as it avoids acquiring locks.
    pub fn find(&self, value: &T) -> Option<Interned<T, Storage>> {
        let hash = self.hasher.hash_one(value);
        self.map
            .find(hash, |&i| self.vec[i as usize].borrow() == value)
            .map(|id| Interned {
                id: *id,
                _phantom: PhantomData,
            })
    }

    /// Returns the given value's [`Interned`] handle if it is already interned.
    ///
    /// Otherwise, this simply returns [`None`] without adding the value to this
    /// arena.
    ///
    /// Contrary to [`find()`](Self::find), no locks are held internally because
    /// this function already takes an exclusive mutable reference to this
    /// arena.
    pub fn find_mut(&mut self, value: &T) -> Option<Interned<T, Storage>> {
        let hash = self.hasher.hash_one(value);
        #[cfg(not(feature = "sync"))]
        return self
            .map
            .find(hash, |&i| self.vec[i as usize].borrow() == value)
            .map(|id| Interned {
                id: *id,
                _phantom: PhantomData,
            });
        #[cfg(feature = "sync")]
        return self
            .map
            .find_mut(hash, |&i| self.vec[i as usize].borrow() == value)
            .map(|id| Interned {
                id: *id,
                _phantom: PhantomData,
            });
    }
}

#[cfg(feature = "raw")]
impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    pub fn push_mut(&mut self, value: Storage) -> u32 {
        self.push(value)
    }
}

impl<T: ?Sized, Storage> Default for Arena<T, Storage> {
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
            hasher: DefaultHashBuilder::default(),
            #[cfg(feature = "debug")]
            references: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage> Debug for Arena<T, Storage>
where
    T: Debug,
    Storage: Borrow<T>,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_list().entries(self.iter_()).finish()
    }
}

impl<T: ?Sized, Storage> PartialEq for Arena<T, Storage>
where
    T: Eq,
    Storage: Borrow<T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter_().eq(other.iter_())
    }
}

impl<T: ?Sized, Storage> Eq for Arena<T, Storage>
where
    T: Eq,
    Storage: Borrow<T>,
{
}

#[cfg(feature = "get-size2")]
impl<T: ?Sized, Storage> GetSize for Arena<T, Storage>
where
    Storage: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        let heap_size = self.vec.iter().map(|x| x.get_size()).sum::<usize>()
            + self.vec.len() * size_of::<u32>();
        (heap_size, tracker)
    }
}

#[cfg(feature = "debug")]
impl<T: ?Sized, Storage> Arena<T, Storage>
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
impl<T: ?Sized, Storage> Arena<T, Storage> {
    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    /// Interns the given value in this arena.
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    ///
    /// See also [`intern_mut()`](Self::intern_mut), which is more efficient if
    /// you hold a mutable reference to this arena as it avoids acquiring locks.
    #[cfg(feature = "sync")]
    pub fn intern(&self, value: impl Borrow<T> + Into<Storage>) -> Interned<T, Storage> {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value.borrow());
        let id = *self
            .map
            .entry(
                hash,
                |&i| self.vec[i as usize].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i as usize].borrow()),
            )
            .or_insert_with(|| {
                let x: Storage = value.into();
                let id = self.vec.push(x);
                assert!(id <= u32::MAX as usize);
                id as u32
            })
            .get();
        Interned::new(id)
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
    pub fn intern_mut(&mut self, value: impl Borrow<T> + Into<Storage>) -> Interned<T, Storage> {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hasher.hash_one(value.borrow());
        #[cfg(not(feature = "sync"))]
        let entry = self.map.entry(
            hash,
            |&i| self.vec[i as usize].borrow() == value.borrow(),
            |&i| self.hasher.hash_one(self.vec[i as usize].borrow()),
        );
        #[cfg(feature = "sync")]
        let entry = self.map.entry_mut(
            hash,
            |&i| self.vec[i as usize].borrow() == value.borrow(),
            |&i| self.hasher.hash_one(self.vec[i as usize].borrow()),
        );
        let id = *entry
            .or_insert_with(|| {
                let x: Storage = value.into();
                #[cfg(not(feature = "sync"))]
                let id = {
                    let id = self.vec.len();
                    self.vec.push(x);
                    id
                };
                #[cfg(feature = "sync")]
                let id = self.vec.push_mut(x);
                assert!(id <= u32::MAX as usize);
                id as u32
            })
            .get();
        Interned::new(id)
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    ///
    /// Calling this function multiple times with the same value doesn't violate
    /// safety, but the value will be stored multiple times in the arena.
    pub(crate) fn push(&mut self, value: Storage) -> u32 {
        #[cfg(feature = "debug")]
        {
            *self.references.get_mut() += 1;
        }

        let hash = self.hasher.hash_one(value.borrow());

        #[cfg(not(feature = "sync"))]
        let id = {
            let id = self.vec.len();
            self.vec.push(value);
            id
        };
        #[cfg(feature = "sync")]
        let id = self.vec.push_mut(value);
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        #[cfg(not(feature = "sync"))]
        self.map.insert_unique(hash, id, |&i| {
            self.hasher.hash_one(self.vec[i as usize].borrow())
        });
        #[cfg(feature = "sync")]
        self.map.insert_unique_mut(hash, id, |&i| {
            self.hasher.hash_one(self.vec[i as usize].borrow())
        });
        id
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    Storage: Clone,
{
    /// Retrieves the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup_ref()`](Self::lookup_ref) if you only need a
    /// reference.
    pub fn lookup(&self, interned: Interned<T, Storage>) -> Storage {
        self.vec[interned.id as usize].clone()
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    Storage: Borrow<T>,
{
    /// Retrieves a reference to the given [`Interned`] value from this arena.
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup()`](Self::lookup) if you need an owned value.
    pub fn lookup_ref(&self, interned: Interned<T, Storage>) -> &T {
        self.vec[interned.id as usize].borrow()
    }
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage> Serialize for Arena<T, Storage>
where
    T: Serialize,
    Storage: Borrow<T>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter_())
    }
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage> Deserialize<'de> for Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArenaVisitor::new())
    }
}

#[cfg(feature = "serde")]
struct ArenaVisitor<T: ?Sized, Storage> {
    _phantom: PhantomData<fn() -> Arena<T, Storage>>,
}

#[cfg(feature = "serde")]
impl<T: ?Sized, Storage> ArenaVisitor<T, Storage> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, T: ?Sized, Storage> Visitor<'de> for ArenaVisitor<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
{
    type Value = Arena<T, Storage>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut arena = match seq.size_hint() {
            None => Arena::default(),
            Some(size_hint) => Arena::with_capacity(size_hint),
        };

        while let Some(t) = seq.next_element()? {
            arena.push(t);
        }

        Ok(arena)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::borrow::Cow;
    #[cfg(feature = "sync")]
    use std::thread;

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup() {
        let arena: Arena<u32> = Arena::default();
        for i in 0..100 {
            assert_eq!(arena.intern(2 * i).id, i);
        }
        for i in 0..100 {
            assert_eq!(*arena.lookup_ref(Interned::new(i)), 2 * i);
            assert_eq!(arena.lookup(Interned::new(i)), 2 * i);
        }
    }

    #[test]
    fn test_intern_mut_lookup() {
        let mut arena: Arena<u32> = Arena::default();
        for i in 0..100 {
            assert_eq!(arena.intern_mut(2 * i).id, i);
        }
        for i in 0..100 {
            assert_eq!(*arena.lookup_ref(Interned::new(i)), 2 * i);
            assert_eq!(arena.lookup(Interned::new(i)), 2 * i);
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
        let mut arena: Arena<Box<u32>> = Arena::default();
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                assert_eq!(arena.push_mut(Box::new(42 + j)), i * NUM_VALUES + j);
                let id = arena.intern_mut(Box::new(42 + j)).id;
                assert_eq!(id % NUM_VALUES, j);
                assert!(id / NUM_VALUES <= i);
            }
        }
        for i in 0..NUM_ITERS {
            for j in 0..NUM_VALUES {
                assert_eq!(
                    **arena.lookup_ref(Interned::new(i * NUM_VALUES + j)),
                    42 + j
                );
                assert_eq!(*arena.lookup(Interned::new(i * NUM_VALUES + j)), 42 + j);
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
        let arena: Arena<u32, Box<u32>> = Arena::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.len();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(*arena.lookup_ref(Interned::new(last)), last);
                            if len == NUM_ITEMS {
                                break;
                            }
                        }
                    }
                });
            }
            s.spawn(|| {
                for j in 0..NUM_ITEMS as u32 {
                    assert_eq!(arena.intern(j).id, j);
                }
            });
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_writes() {
        let arena: Arena<u32, Box<u32>> = Arena::default();
        thread::scope(|s| {
            s.spawn(|| {
                loop {
                    let len = arena.len();
                    if len > 0 {
                        let last = len as u32 - 1;
                        assert_eq!(*arena.lookup_ref(Interned::new(last)), last);
                        if len == NUM_ITEMS {
                            break;
                        }
                    }
                }
            });
            for _ in 0..NUM_WRITERS {
                s.spawn(|| {
                    for j in 0..NUM_ITEMS as u32 {
                        assert_eq!(arena.intern(j).id, j);
                    }
                });
            }
        });
    }

    #[cfg(feature = "sync")]
    #[test]
    fn test_intern_lookup_concurrent_readwrites() {
        let arena: Arena<u32, Box<u32>> = Arena::default();
        thread::scope(|s| {
            for _ in 0..NUM_READERS {
                s.spawn(|| {
                    loop {
                        let len = arena.len();
                        if len > 0 {
                            let last = len as u32 - 1;
                            assert_eq!(*arena.lookup_ref(Interned::new(last)), last);
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
                        assert_eq!(arena.intern(j).id, j);
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
        assert_eq!(arena.intern(key).id, 0);

        let key: String = "world".into();
        assert_eq!(arena.intern(key).id, 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(arena.intern(key).id, 0);

        let key: Box<str> = "world".into();
        assert_eq!(arena.intern(key).id, 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(arena.intern(key).id, 2);
    }

    #[test]
    fn test_boxed_str_interner_mut() {
        let mut arena: Arena<str, Box<str>> = Arena::default();

        let key: &str = "Hello";
        assert_eq!(arena.intern_mut(key).id, 0);

        let key: String = "world".into();
        assert_eq!(arena.intern_mut(key).id, 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(arena.intern_mut(key).id, 0);

        let key: Box<str> = "world".into();
        assert_eq!(arena.intern_mut(key).id, 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(arena.intern_mut(key).id, 2);
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
