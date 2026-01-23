//! This crate offers an efficient and concurrent
//! [interning](https://en.wikipedia.org/wiki/Interning_(computer_science)) API
//! over generic data.
//!
//! Here are its main features.
//!
//! - **Generic**: You can intern any data type that implements [`Hash`] and
//!   [`Eq`], not just strings. The interned type doesn't even have to be
//!   [`Sized`] (for example [`str`]), as long as you provide a [`Sized`]
//!   storage type (such as `Box<str>`) that can be borrowed as the interned
//!   type.
//! - **Efficient**: Each [`Interned`] value contains only a 32-bit index. The
//!   corresponding [`Arena`] stores each value directly in an [`AppendVec`],
//!   plus the 32-bit index in a raw hash table ([`DashTable`]). To intern a
//!   value of type `T` using storage type `S`, you can pass any type that
//!   implements [`Borrow<T>`](Borrow) and [`Into<S>`](Into), which allows
//!   avoiding unnecessary copies. For example, in an `Arena<str, Box<str>>` you
//!   can intern many string types: `&str`, `String`, `Box<str>`, `Cow<'_,
//!   str>`, etc.
//! - **Concurrent**: The [`Arena`] is [`Sync`], and allows simultaneous reads
//!   and writes. More specifically, retrieving values via
//!   [`Interned::lookup()`] and [`Interned::lookup_ref()`] is always wait-free,
//!   even when a write happens concurrently! This is thanks to the underlying
//!   [`AppendVec`] implementation. However, only one write (using
//!   [`Interned::from()`]) can happen at a time on a given arena, due to an
//!   exclusive write lock.

#![forbid(missing_docs, unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use appendvec::AppendVec;
use dashtable::DashTable;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use hashbrown::DefaultHashBuilder;
#[cfg(feature = "serde")]
use serde::de::{SeqAccess, Visitor};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
#[cfg(feature = "get-size2")]
use std::mem::size_of;
#[cfg(feature = "debug")]
use std::sync::atomic::{self, AtomicUsize};

/// A handle to an interned value in an [`Arena`].
///
/// This is generic over the logical value type `T` as well as its `Storage`
/// type, that needs to be [`Sized`]. For [`Sized`] values, `Storage = T` is a
/// good default that incurs no overhead. For non-[`Sized`] values such as
/// [`str`], you need to specify a [`Sized`] storage type, such as `Box<T>`.
pub struct Interned<T: ?Sized, Storage = T> {
    id: u32,
    _phantom: PhantomData<fn() -> (*const T, *const Storage)>,
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
    /// [`lookup()`](lookup) and [`lookup_ref()`](lookup_ref) APIs, unless you
    /// really know what you're doing.
    pub fn id(&self) -> u32 {
        self.id
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    /// Interns the given value in the given [`Arena`].
    ///
    /// If the value was already interned in this arena, it will simply be
    /// borrowed to retrieve its interning index. Otherwise it will then be
    /// converted to store it into the arena.
    pub fn from(arena: &Arena<T, Storage>, value: impl Borrow<T> + Into<Storage>) -> Self {
        let id = arena.intern(value);
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    Storage: Clone,
{
    /// Retrieves this interned value from the given [`Arena`].
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup_ref()`](lookup_ref) if you only need a reference.
    pub fn lookup(&self, arena: &Arena<T, Storage>) -> Storage {
        arena.lookup(self.id)
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    Storage: Borrow<T>,
{
    /// Retrieves a reference to this interned value from the given [`Arena`].
    ///
    /// The caller is responsible for ensuring that the same arena was used to
    /// intern this value, otherwise an arbitrary value will be returned or
    /// a panic will happen.
    ///
    /// See also [`lookup()`](lookup) if you need an owned value.
    pub fn lookup_ref<'a>(&self, arena: &'a Arena<T, Storage>) -> &'a T {
        arena.lookup_ref(self.id)
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
        let id = deserializer.deserialize_u32(U32Visitor)?;
        Ok(Self {
            id,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "serde")]
struct U32Visitor;

#[cfg(feature = "serde")]
impl Visitor<'_> for U32Visitor {
    type Value = u32;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer between 0 and 2^32")
    }

    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(u32::from(value))
    }

    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(u32::from(value))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }
}

/// Interning arena for values of type `T`, storing them with the given
/// `Storage` type (that needs to be [`Sized`]).
///
/// For [`Sized`] values, `Storage = T` is a good default that incurs no
/// overhead. For non-[`Sized`] values such as [`str`], you need to specify a
/// [`Sized`] storage type, such as `Box<T>`.
pub struct Arena<T: ?Sized, Storage = T> {
    vec: AppendVec<Storage>,
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    #[cfg(feature = "debug")]
    references: AtomicUsize,
    _phantom: PhantomData<fn() -> *const T>,
}

impl<T: ?Sized, Storage> Default for Arena<T, Storage> {
    fn default() -> Self {
        Self {
            vec: AppendVec::new(),
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
        fmt.debug_list()
            .entries(self.vec.iter().map(|x| x.borrow()))
            .finish()
    }
}

impl<T: ?Sized, Storage> PartialEq for Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.vec
            .iter()
            .map(|x| x.borrow())
            .eq(other.vec.iter().map(|x| x.borrow()))
    }
}

impl<T: ?Sized, Storage> Eq for Arena<T, Storage>
where
    T: Eq + Hash,
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
        let len = self.len();
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
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn intern(&self, value: impl Borrow<T> + Into<Storage>) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value.borrow());
        *self
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
            .get()
    }

    /// Unconditionally push a value, without validating that it's already
    /// interned.
    #[cfg(feature = "serde")]
    fn push(&mut self, value: Storage) -> u32 {
        #[cfg(feature = "debug")]
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value.borrow());

        let id = self.vec.push_mut(value);
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        self.map.insert_unique(hash, id, |&i| {
            self.hasher.hash_one(self.vec[i as usize].borrow())
        });

        id
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    Storage: Clone,
{
    fn lookup(&self, id: u32) -> Storage {
        self.vec[id as usize].clone()
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    Storage: Borrow<T>,
{
    fn lookup_ref(&self, id: u32) -> &T {
        self.vec[id as usize].borrow()
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
        serializer.collect_seq(self.vec.iter().map(|x| x.borrow()))
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
            Some(size_hint) => Arena {
                vec: AppendVec::with_capacity(size_hint),
                map: DashTable::with_capacity(size_hint),
                hasher: DefaultHashBuilder::default(),
                #[cfg(feature = "debug")]
                references: AtomicUsize::new(0),
                _phantom: PhantomData,
            },
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

    #[test]
    fn test_str_interner() {
        let arena: Arena<str, Box<str>> = Arena::default();

        let key: &str = "Hello";
        assert_eq!(arena.intern(key), 0);

        let key: String = "world".into();
        assert_eq!(arena.intern(key), 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(arena.intern(key), 0);

        let key: Box<str> = "world".into();
        assert_eq!(arena.intern(key), 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(arena.intern(key), 2);
    }
}
