use super::{Arena, ArenaSlice, ArenaStr, Index, Interned, InternedSlice, InternedStr};
use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
#[cfg(feature = "retain")]
use bit_set::BitSet;
use core::borrow::Borrow;
use core::cmp::Ordering;
use core::hash::{BuildHasher, Hash};
#[cfg(feature = "retain")]
use core::marker::PhantomData;
use either::Either;
#[cfg(feature = "retain")]
use hashbrown::DefaultHashBuilder;

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Ord,
    Storage: Borrow<T>,
    I: Index,
{
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping<I> {
        let mut mapping: Vec<I> = (0..self.len()).map(I::from_usize).collect();
        mapping.sort_by_cached_key(|i| self.lookup_ref(Interned::new(*i)));
        let reverse = ReverseMapping::new(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    I: Index,
{
    /// Returns the identity mapping of the items in this arena.
    ///
    /// This can be useful to rehash the arena.
    pub fn identity(&self) -> Mapping<I> {
        Mapping::identity(I::from_usize(self.len()))
    }

    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(Interned<T, Storage, H, I>) -> bool) -> Mapping<I> {
        Mapping::retain(self.len(), |i| filter(Interned::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    ///
    /// See also [`retain_builder()`](Self::retain_builder) if you need a more
    /// flexible way of adding items to retain.
    #[cfg(feature = "retain")]
    pub fn retain_values(
        &self,
        values: impl Iterator<Item = Interned<T, Storage, H, I>>,
    ) -> Mapping<I> {
        let mut builder = self.retain_builder();
        for v in values {
            builder.insert(v);
        }
        builder.build()
    }

    /// Returns a builder allowing to select items to retain, and create an
    /// arena containing only these.
    #[cfg(feature = "retain")]
    pub fn retain_builder(&self) -> RetainBuilder<T, Storage, H, I> {
        let len = self.len();
        RetainBuilder {
            len,
            retained: BitSet::with_capacity(len),
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage, H, I> Arena<T, Storage, H, I>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Clone,
    I: Index,
{
    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map_rehash()`](Self::map_rehash) to use a different hash
    /// function in the resulting arena.
    pub fn map(&self, mapping: &ReverseMapping<I>) -> Self
    where
        H: Default + BuildHasher,
    {
        self.map_rehash(mapping)
    }

    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map()`](Self::map) if the resulting arena uses the same hash
    /// function.
    pub fn map_rehash<HH>(&self, mapping: &ReverseMapping<I>) -> Arena<T, Storage, HH, I>
    where
        HH: Default + BuildHasher,
    {
        let mut arena = Arena::with_capacity(mapping.len());
        for i in mapping.iter() {
            arena.push(self.lookup(Interned::new(i)));
        }
        arena
    }

    /// Returns a re-ordered version of this arena based on the given mapping,
    /// where each item is additionally transformed according to the given
    /// function `f`.
    ///
    /// The transformation function should be injective, i.e. different inputs
    /// should map to different outputs. Otherwise a value may appear twice in
    /// the resulting arena.
    ///
    /// See also [`map2_rehash()`](Self::map2_rehash) to use a different hash
    /// function in the resulting arena.
    pub fn map2(&self, mapping: &ReverseMapping<I>, f: impl Fn(&T) -> Storage) -> Self
    where
        H: Default + BuildHasher,
    {
        self.map2_rehash(mapping, f)
    }

    /// Returns a re-ordered version of this arena based on the given mapping,
    /// where each item is additionally transformed according to the given
    /// function `f`.
    ///
    /// The transformation function should be injective, i.e. different inputs
    /// should map to different outputs. Otherwise a value may appear twice in
    /// the resulting arena.
    ///
    /// See also [`map2()`](Self::map2) if the resulting arena uses the same
    /// hash function.
    pub fn map2_rehash<HH>(
        &self,
        mapping: &ReverseMapping<I>,
        f: impl Fn(&T) -> Storage,
    ) -> Arena<T, Storage, HH, I>
    where
        HH: Default + BuildHasher,
    {
        let mut arena = Arena::with_capacity(mapping.len());
        for i in mapping.iter() {
            arena.push(f(self.lookup_ref(Interned::new(i))));
        }
        arena
    }
}

/// A builder to select items to retain in an [`Arena`].
///
/// This struct is created by the [`retain_builder()`](Arena::retain_builder)
/// method on [`Arena`].
#[cfg(feature = "retain")]
pub struct RetainBuilder<T: ?Sized, Storage = T, H = DefaultHashBuilder, I = u32> {
    len: usize,
    retained: BitSet,
    _phantom: PhantomData<Interned<T, Storage, H, I>>,
}

#[cfg(feature = "retain")]
impl<T: ?Sized, Storage, H, I> RetainBuilder<T, Storage, H, I>
where
    I: Index,
{
    /// Marks the given item as retained.
    ///
    /// Returns [`true`] if the item is newly inserted and [`false`] if it was
    /// already inserted before.
    pub fn insert(&mut self, value: Interned<T, Storage, H, I>) -> bool {
        self.retained.insert(value.id_().to_usize())
    }

    /// Returns a mapping to build an [`Arena`] containing only the items that
    /// have been retained via [`insert()`](Self::insert).
    pub fn build(self) -> Mapping<I> {
        Mapping::retain(self.len, |i: I| self.retained.contains(i.to_usize()))
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Ord,
    I: Index,
{
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping<I> {
        let mut mapping: Vec<I> = (0..self.slices()).map(I::from_usize).collect();
        mapping.sort_by_cached_key(|i| CustomSliceOrd(self.lookup(InternedSlice::new(*i))));
        let reverse = ReverseMapping::new(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    I: Index,
{
    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(InternedSlice<T, H, I>) -> bool) -> Mapping<I> {
        Mapping::retain(self.slices(), |i| filter(InternedSlice::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    ///
    /// See also [`retain_builder()`](Self::retain_builder) if you need a more
    /// flexible way of adding items to retain.
    #[cfg(feature = "retain")]
    pub fn retain_values(
        &self,
        values: impl Iterator<Item = InternedSlice<T, H, I>>,
    ) -> Mapping<I> {
        let mut builder = self.retain_builder();
        for v in values {
            builder.insert(v);
        }
        builder.build()
    }

    /// Returns a builder allowing to select items to retain, and create an
    /// arena containing only these.
    #[cfg(feature = "retain")]
    pub fn retain_builder(&self) -> RetainSliceBuilder<T, H, I> {
        let len = self.slices();
        RetainSliceBuilder {
            len,
            retained: BitSet::with_capacity(len),
            _phantom: PhantomData,
        }
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Default + Clone + Eq + Hash,
    I: Index,
{
    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map_rehash()`](Self::map_rehash) to use a different hash
    /// function in the resulting arena.
    pub fn map(&self, mapping: &ReverseMapping<I>) -> Self
    where
        H: Default + BuildHasher,
    {
        self.map_rehash(mapping)
    }

    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map()`](Self::map) if the resulting arena uses the same hash
    /// function.
    pub fn map_rehash<HH>(&self, mapping: &ReverseMapping<I>) -> ArenaSlice<T, HH, I>
    where
        HH: Default + BuildHasher,
    {
        let mut arena = ArenaSlice::with_capacity(mapping.len(), self.items());
        for i in mapping.iter() {
            arena.push(self.lookup(InternedSlice::new(i)));
        }
        arena
    }
}

impl<T, H, I> ArenaSlice<T, H, I>
where
    T: Default + Eq + Hash,
    I: Index,
{
    /// Returns a re-ordered version of this arena based on the given mapping,
    /// where each slice element is additionally transformed according to
    /// the given function `f`.
    ///
    /// The transformation function should be injective, i.e. different inputs
    /// should map to different outputs. Otherwise a value may appear twice in
    /// the resulting arena.
    ///
    /// See also [`map2_rehash()`](Self::map2_rehash) to use a different hash
    /// function in the resulting arena.
    pub fn map2(&self, mapping: &ReverseMapping<I>, f: impl Fn(&T) -> T) -> Self
    where
        H: Default + BuildHasher,
    {
        self.map2_rehash(mapping, f)
    }

    /// Returns a re-ordered version of this arena based on the given mapping,
    /// where each slice element is additionally transformed according to
    /// the given function `f`.
    ///
    /// The transformation function should be injective, i.e. different inputs
    /// should map to different outputs. Otherwise a value may appear twice in
    /// the resulting arena.
    ///
    /// See also [`map2()`](Self::map2) if the resulting arena uses the same
    /// hash function.
    pub fn map2_rehash<HH>(
        &self,
        mapping: &ReverseMapping<I>,
        f: impl Fn(&T) -> T,
    ) -> ArenaSlice<T, HH, I>
    where
        HH: Default + BuildHasher,
    {
        let mut arena = ArenaSlice::with_capacity(mapping.len(), self.items());
        for i in mapping.iter() {
            let slice = self.lookup(InternedSlice::new(i));
            let iter = slice.iter().map(&f);
            // SAFETY: The iterator length is trusted, as it's a simple mapping on a slice
            // iterator.
            unsafe { arena.push_iter_mut_(iter) };
        }
        arena
    }
}

/// A builder to select items to retain in an [`ArenaSlice`].
///
/// This struct is created by the
/// [`retain_builder()`](ArenaSlice::retain_builder) method on [`ArenaSlice`].
#[cfg(feature = "retain")]
pub struct RetainSliceBuilder<T, H = DefaultHashBuilder, I = u32> {
    len: usize,
    retained: BitSet,
    _phantom: PhantomData<InternedSlice<T, H, I>>,
}

#[cfg(feature = "retain")]
impl<T, H, I> RetainSliceBuilder<T, H, I>
where
    I: Index,
{
    /// Marks the given item as retained.
    ///
    /// Returns [`true`] if the item is newly inserted and [`false`] if it was
    /// already inserted before.
    pub fn insert(&mut self, value: InternedSlice<T, H, I>) -> bool {
        self.retained.insert(value.id_().to_usize())
    }

    /// Returns a mapping to build an [`ArenaSlice`] containing only the items
    /// that have been retained via [`insert()`](Self::insert).
    pub fn build(self) -> Mapping<I> {
        Mapping::retain(self.len, |i: I| self.retained.contains(i.to_usize()))
    }
}

#[derive(PartialEq, Eq)]
struct CustomSliceOrd<'a, T>(&'a [T]);

impl<T: Ord> PartialOrd for CustomSliceOrd<'_, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for CustomSliceOrd<'_, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(other.0))
    }
}

impl<H, I> ArenaStr<H, I>
where
    I: Index,
{
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping<I> {
        let mut mapping: Vec<I> = (0..self.strings()).map(I::from_usize).collect();
        mapping.sort_by_cached_key(|i| CustomStrOrd(self.lookup(InternedStr::new(*i))));
        let reverse = ReverseMapping::new(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }

    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(InternedStr<H, I>) -> bool) -> Mapping<I> {
        Mapping::retain(self.strings(), |i| filter(InternedStr::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    ///
    /// See also [`retain_builder()`](Self::retain_builder) if you need a more
    /// flexible way of adding items to retain.
    #[cfg(feature = "retain")]
    pub fn retain_values(&self, values: impl Iterator<Item = InternedStr<H, I>>) -> Mapping<I> {
        let mut builder = self.retain_builder();
        for v in values {
            builder.insert(v);
        }
        builder.build()
    }

    /// Returns a builder allowing to select items to retain, and create an
    /// arena containing only these.
    #[cfg(feature = "retain")]
    pub fn retain_builder(&self) -> RetainStrBuilder<H, I> {
        let len = self.strings();
        RetainStrBuilder {
            len,
            retained: BitSet::with_capacity(len),
            _phantom: PhantomData,
        }
    }
}

impl<H, I> ArenaStr<H, I>
where
    I: Index,
{
    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map_rehash()`](Self::map_rehash) to use a different hash
    /// function in the resulting arena.
    pub fn map(&self, mapping: &ReverseMapping<I>) -> Self
    where
        H: Default + BuildHasher,
    {
        self.map_rehash(mapping)
    }

    /// Returns a re-ordered version of this arena based on the given mapping.
    ///
    /// See also [`map()`](Self::map) if the resulting arena uses the same hash
    /// function.
    pub fn map_rehash<HH>(&self, mapping: &ReverseMapping<I>) -> ArenaStr<HH, I>
    where
        HH: Default + BuildHasher,
    {
        let mut arena = ArenaStr::with_capacity(mapping.len(), self.bytes());
        for i in mapping.iter() {
            arena.push(self.lookup(InternedStr::new(i)));
        }
        arena
    }
}

/// A builder to select items to retain in an [`ArenaStr`].
///
/// This struct is created by the [`retain_builder()`](ArenaStr::retain_builder)
/// method on [`ArenaStr`].
#[cfg(feature = "retain")]
pub struct RetainStrBuilder<H = DefaultHashBuilder, I = u32> {
    len: usize,
    retained: BitSet,
    _phantom: PhantomData<InternedStr<H, I>>,
}

#[cfg(feature = "retain")]
impl<H, I> RetainStrBuilder<H, I>
where
    I: Index,
{
    /// Marks the given item as retained.
    ///
    /// Returns [`true`] if the item is newly inserted and [`false`] if it was
    /// already inserted before.
    pub fn insert(&mut self, value: InternedStr<H, I>) -> bool {
        self.retained.insert(value.id_().to_usize())
    }

    /// Returns a mapping to build an [`ArenaStr`] containing only the items
    /// that have been retained via [`insert()`](Self::insert).
    pub fn build(self) -> Mapping<I> {
        Mapping::retain(self.len, |i: I| self.retained.contains(i.to_usize()))
    }
}

#[derive(PartialEq, Eq)]
struct CustomStrOrd<'a>(&'a str);

impl PartialOrd for CustomStrOrd<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CustomStrOrd<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(other.0))
    }
}

/// A mapping to re-order items in an [`Arena`], [`ArenaSlice`] or [`ArenaStr`].
pub struct Mapping<I = u32> {
    /// Forward mapping, to map interned handles.
    pub forward: ForwardMapping<I>,
    /// Reverse mapping, to re-order the arena.
    pub reverse: ReverseMapping<I>,
}

impl<I: Index> Mapping<I> {
    /// Creates a new identity mapping with the given number of items.
    pub fn identity(count: I) -> Self {
        Self {
            forward: ForwardMapping::identity(count),
            reverse: ReverseMapping::identity(count),
        }
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.forward.is_identity()
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_rehash()`](Self::map_rehash) if the resulting arena uses
    /// a different hash function.
    pub fn map<T: ?Sized, Storage, H>(
        &self,
        index: Interned<T, Storage, H, I>,
    ) -> Interned<T, Storage, H, I> {
        self.forward.map(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_slice_rehash()`](Self::map_slice_rehash) if the resulting
    /// arena uses a different hash function.
    pub fn map_slice<T, H>(&self, index: InternedSlice<T, H, I>) -> InternedSlice<T, H, I> {
        self.forward.map_slice(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_str_rehash()`](Self::map_str_rehash) if the resulting
    /// arena uses a different hash function.
    pub fn map_str<H>(&self, index: InternedStr<H, I>) -> InternedStr<H, I> {
        self.forward.map_str(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map()`](Self::map) if the resulting arena uses the same hash
    /// function.
    pub fn map_rehash<T: ?Sized, Storage, H, HH>(
        &self,
        index: Interned<T, Storage, H, I>,
    ) -> Interned<T, Storage, HH, I> {
        self.forward.map_rehash(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_slice()`](Self::map_slice) if the resulting arena uses
    /// the same hash function.
    pub fn map_slice_rehash<T, H, HH>(
        &self,
        index: InternedSlice<T, H, I>,
    ) -> InternedSlice<T, HH, I> {
        self.forward.map_slice_rehash(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_str()`](Self::map_str) if the resulting arena uses the
    /// same hash function.
    pub fn map_str_rehash<H, HH>(&self, index: InternedStr<H, I>) -> InternedStr<HH, I> {
        self.forward.map_str_rehash(index)
    }

    fn retain(len: usize, filter: impl Fn(I) -> bool) -> Self {
        let mut reverse = Vec::new();
        let mut forward = Vec::with_capacity(len);
        let mut next = I::ZERO;
        let mut is_identity = true;

        for i in (0..len).map(I::from_usize) {
            if filter(i) {
                reverse.push(i);
                forward.push(next);
                next.incr();
            } else {
                forward.push(I::MAX);
                is_identity = false;
            }
        }

        let reverse = ReverseMapping::new(reverse.into_boxed_slice());
        let forward = if is_identity {
            MappingImpl::Identity(I::from_usize(len))
        } else {
            MappingImpl::Map(forward.into_boxed_slice())
        };
        Self {
            forward: ForwardMapping(forward),
            reverse,
        }
    }
}

/// A mapping to re-order items in an [`Arena`], [`ArenaSlice`] or [`ArenaStr`].
#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub struct ReverseMapping<I = u32>(MappingImpl<I>);

impl<I: Index> ReverseMapping<I> {
    /// Creates a new identity mapping with the given number of items.
    fn identity(count: I) -> Self {
        Self(MappingImpl::Identity(count))
    }

    fn new(map: Box<[I]>) -> Self {
        Self(MappingImpl::new(map))
    }

    /// Returns the number of mapped items.
    fn len(&self) -> usize {
        self.0.len().to_usize()
    }

    fn reverse(&self) -> ForwardMapping<I> {
        ForwardMapping(self.0.reverse())
    }

    /// Returns the mapped indices in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = I> {
        self.0.iter()
    }
}

/// A mapping to re-order items in an [`Arena`], [`ArenaSlice`] or [`ArenaStr`].
#[cfg_attr(test, derive(Clone, PartialEq, Eq, Debug))]
pub struct ForwardMapping<I = u32>(MappingImpl<I>);

impl<I: Index> ForwardMapping<I> {
    /// Creates a new identity mapping with the given number of items.
    pub fn identity(count: I) -> Self {
        Self(MappingImpl::Identity(count))
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.0.is_identity()
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_rehash()`](Self::map_rehash) if the resulting arena uses
    /// a different hash function.
    pub fn map<T: ?Sized, Storage, H>(
        &self,
        index: Interned<T, Storage, H, I>,
    ) -> Interned<T, Storage, H, I> {
        self.map_rehash(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_slice_rehash()`](Self::map_slice_rehash) if the resulting
    /// arena uses a different hash function.
    pub fn map_slice<T, H>(&self, index: InternedSlice<T, H, I>) -> InternedSlice<T, H, I> {
        self.map_slice_rehash(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_str_rehash()`](Self::map_str_rehash) if the resulting
    /// arena uses a different hash function.
    pub fn map_str<H>(&self, index: InternedStr<H, I>) -> InternedStr<H, I> {
        self.map_str_rehash(index)
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map()`](Self::map) if the resulting arena uses the same hash
    /// function.
    pub fn map_rehash<T: ?Sized, Storage, H, HH>(
        &self,
        index: Interned<T, Storage, H, I>,
    ) -> Interned<T, Storage, HH, I> {
        Interned::new(self.0.at(index.id_()))
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_slice()`](Self::map_slice) if the resulting arena uses
    /// the same hash function.
    pub fn map_slice_rehash<T, H, HH>(
        &self,
        index: InternedSlice<T, H, I>,
    ) -> InternedSlice<T, HH, I> {
        InternedSlice::new(self.0.at(index.id_()))
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    ///
    /// See also [`map_str()`](Self::map_str) if the resulting arena uses the
    /// same hash function.
    pub fn map_str_rehash<H, HH>(&self, index: InternedStr<H, I>) -> InternedStr<HH, I> {
        InternedStr::new(self.0.at(index.id_()))
    }

    /// Returned a composition of this mapping followed by the other mapping.
    ///
    /// The composition is such that for any interned handle `index`,
    /// `map1.compose(map2).map(index)` is equal to `map2.map(map1.map(index))`.
    pub fn compose(self, other: Self) -> Self {
        Self(self.0.compose(other.0))
    }

    /// Returns the number of items that are remapped by this mapping.
    #[cfg(feature = "debug")]
    pub fn count_remapped(&self) -> usize {
        self.0.count_remapped()
    }
}

#[cfg_attr(test, derive(Clone, PartialEq, Eq, Debug))]
enum MappingImpl<I> {
    Identity(I),
    Map(Box<[I]>),
}

impl<I: Index> MappingImpl<I> {
    fn new(map: Box<[I]>) -> Self {
        // Check if the map is the identity.
        if map.iter().enumerate().all(|(i, j)| i == j.to_usize()) {
            MappingImpl::Identity(I::from_usize(map.len()))
        } else {
            MappingImpl::Map(map)
        }
    }

    /// Checks wether this mapping is the identity.
    fn is_identity(&self) -> bool {
        match self {
            Self::Identity(_) => true,
            Self::Map(_) => false,
        }
    }

    fn len(&self) -> I {
        match self {
            Self::Identity(len) => *len,
            Self::Map(map) => I::from_usize(map.len()),
        }
    }

    fn at(&self, index: I) -> I {
        match self {
            MappingImpl::Identity(_) => index,
            MappingImpl::Map(map) => map[index.to_usize()],
        }
    }

    fn compose(self, other: MappingImpl<I>) -> Self {
        let len = self.len();
        assert_eq!(len, other.len());
        match (self, other) {
            (MappingImpl::Identity(len), MappingImpl::Identity(_)) => MappingImpl::Identity(len),
            (MappingImpl::Map(map), MappingImpl::Identity(_))
            | (MappingImpl::Identity(_), MappingImpl::Map(map)) => MappingImpl::Map(map),
            (MappingImpl::Map(left), MappingImpl::Map(right)) => {
                let map = left.iter().map(|i| right[i.to_usize()]).collect();
                Self::new(map)
            }
        }
    }

    fn reverse(&self) -> MappingImpl<I> {
        match self {
            MappingImpl::Identity(len) => MappingImpl::Identity(*len),
            MappingImpl::Map(map) => {
                let mut reverse = vec![I::ZERO; map.len()];
                for i in 0..map.len() {
                    reverse[map[i].to_usize()] = I::from_usize(i);
                }
                MappingImpl::Map(reverse.into_boxed_slice())
            }
        }
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = I> {
        match self {
            Self::Identity(len) => Either::Left((0..len.to_usize()).map(I::from_usize)),
            Self::Map(map) => Either::Right(map.iter().copied()),
        }
    }

    /// Returns the number of items that are remapped by this mapping.
    #[cfg(feature = "debug")]
    fn count_remapped(&self) -> usize {
        match self {
            Self::Identity(_) => 0,
            Self::Map(map) => map
                .iter()
                .enumerate()
                .filter(|&(i, j)| i != j.to_usize())
                .count(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    type InternedU32 = Interned<u32>;

    #[test]
    fn arena_str_sort() {
        let mut arena: ArenaStr = ArenaStr::default();
        arena.push("bbbb");
        arena.push("dd");
        arena.push("e");
        arena.push("aaaaa");
        arena.push("ccc");

        let mapping = arena.sort();
        assert!(!mapping.is_identity());
        assert_eq!(
            mapping.reverse,
            ReverseMapping(MappingImpl::Map(vec![2, 1, 4, 0, 3].into_boxed_slice()))
        );
        assert_eq!(
            mapping.forward,
            ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()))
        );
    }

    #[test]
    fn arena_str_sort_identity() {
        let mut arena: ArenaStr = ArenaStr::default();
        arena.push("e");
        arena.push("dd");
        arena.push("ccc");
        arena.push("bbbb");
        arena.push("aaaaa");

        let mapping = arena.sort();
        assert!(mapping.is_identity());
        assert_eq!(mapping.reverse, ReverseMapping(MappingImpl::Identity(5)));
        assert_eq!(mapping.forward, ForwardMapping(MappingImpl::Identity(5)));
    }

    #[test]
    fn arena_str_sort_map() {
        let mut arena: ArenaStr = ArenaStr::default();
        arena.push("bbbb");
        arena.push("dd");
        arena.push("e");
        arena.push("aaaaa");
        arena.push("ccc");

        let mapping = arena.sort();
        let sorted: ArenaStr = arena.map(&mapping.reverse);

        let mut expected = ArenaStr::default();
        expected.push("e");
        expected.push("dd");
        expected.push("ccc");
        expected.push("bbbb");
        expected.push("aaaaa");

        assert_eq!(sorted, expected);
    }

    #[test]
    fn arena_str_retain_identity() {
        let mut arena: ArenaStr = ArenaStr::default();
        arena.push("e");
        arena.push("dd");
        arena.push("ccc");
        arena.push("bbbb");
        arena.push("aaaaa");

        let mapping = arena.retain(|i| arena.lookup(i).len() <= 3);
        assert!(!mapping.is_identity());
        assert_eq!(mapping.reverse, ReverseMapping(MappingImpl::Identity(3)));
        assert_eq!(
            mapping.forward,
            ForwardMapping(MappingImpl::Map(Box::new([0, 1, 2, u32::MAX, u32::MAX])))
        );
    }

    #[test]
    fn arena_str_retain() {
        let mut arena: ArenaStr = ArenaStr::default();
        let _ = arena.intern_mut("bbbb");
        let d = arena.intern_mut("dd");
        let e = arena.intern_mut("e");
        let _ = arena.intern_mut("aaaaa");
        let c = arena.intern_mut("ccc");

        let mapping = arena.retain(|i| arena.lookup(i).len() <= 3);
        let filtered: ArenaStr = arena.map(&mapping.reverse);

        let mut expected = ArenaStr::default();
        expected.push("dd");
        expected.push("e");
        expected.push("ccc");

        assert_eq!(filtered, expected);

        let cc = mapping.forward.map_str(c);
        let dd = mapping.forward.map_str(d);
        let ee = mapping.forward.map_str(e);
        assert_eq!(filtered.lookup(cc), "ccc");
        assert_eq!(filtered.lookup(dd), "dd");
        assert_eq!(filtered.lookup(ee), "e");
    }

    #[cfg(feature = "retain")]
    #[test]
    fn arena_str_retain_values() {
        let mut arena: ArenaStr = ArenaStr::default();
        let _ = arena.intern_mut("bbbb");
        let d = arena.intern_mut("dd");
        let e = arena.intern_mut("e");
        let _ = arena.intern_mut("aaaaa");
        let c = arena.intern_mut("ccc");

        let mapping = arena.retain_values([d, e, c].into_iter());
        let filtered: ArenaStr = arena.map(&mapping.reverse);

        let mut expected = ArenaStr::default();
        expected.push("dd");
        expected.push("e");
        expected.push("ccc");

        assert_eq!(filtered, expected);

        let cc = mapping.forward.map_str(c);
        let dd = mapping.forward.map_str(d);
        let ee = mapping.forward.map_str(e);
        assert_eq!(filtered.lookup(cc), "ccc");
        assert_eq!(filtered.lookup(dd), "dd");
        assert_eq!(filtered.lookup(ee), "e");
    }

    #[cfg(feature = "retain")]
    #[test]
    fn arena_str_retain_builder() {
        let mut arena: ArenaStr = ArenaStr::default();
        let _ = arena.intern_mut("bbbb");
        let d = arena.intern_mut("dd");
        let e = arena.intern_mut("e");
        let _ = arena.intern_mut("aaaaa");
        let c = arena.intern_mut("ccc");

        let mut builder = arena.retain_builder();
        assert!(builder.insert(d));
        assert!(builder.insert(e));
        assert!(builder.insert(c));
        assert!(!builder.insert(e));
        let mapping = builder.build();
        let filtered: ArenaStr = arena.map(&mapping.reverse);

        let mut expected = ArenaStr::default();
        expected.push("dd");
        expected.push("e");
        expected.push("ccc");

        assert_eq!(filtered, expected);

        let cc = mapping.forward.map_str(c);
        let dd = mapping.forward.map_str(d);
        let ee = mapping.forward.map_str(e);
        assert_eq!(filtered.lookup(cc), "ccc");
        assert_eq!(filtered.lookup(dd), "dd");
        assert_eq!(filtered.lookup(ee), "e");
    }

    #[test]
    fn reverse_mapping_iter() {
        let mapping = ReverseMapping(MappingImpl::<u32>::new(Box::new([2, 1, 4, 0, 3])));
        assert_eq!(mapping.iter().collect::<Vec<_>>(), vec![2, 1, 4, 0, 3]);
    }

    #[test]
    fn reverse_mapping_reverse() {
        let mapping = ReverseMapping(MappingImpl::<u32>::new(Box::new([2, 1, 4, 0, 3])));
        assert_eq!(
            mapping.reverse(),
            ForwardMapping(MappingImpl::Map(Box::new([3, 1, 0, 4, 2])))
        );

        let mapping = ReverseMapping(MappingImpl::<u32>::new(Box::new([0, 1, 2, 3, 4])));
        assert_eq!(mapping.reverse(), ForwardMapping(MappingImpl::Identity(5)));
    }

    #[test]
    fn forward_mapping_map() {
        let mapping = ForwardMapping(MappingImpl::<u32>::Map(Box::new([3, 1, 0, 4, 2])));
        assert_eq!(mapping.map(InternedU32::new(0)), InternedU32::new(3));
        assert_eq!(mapping.map(InternedU32::new(1)), InternedU32::new(1));
        assert_eq!(mapping.map(InternedU32::new(2)), InternedU32::new(0));
        assert_eq!(mapping.map(InternedU32::new(3)), InternedU32::new(4));
        assert_eq!(mapping.map(InternedU32::new(4)), InternedU32::new(2));

        let mapping = ForwardMapping(MappingImpl::Identity(5));
        assert_eq!(mapping.map(InternedU32::new(0)), InternedU32::new(0));
        assert_eq!(mapping.map(InternedU32::new(1)), InternedU32::new(1));
        assert_eq!(mapping.map(InternedU32::new(2)), InternedU32::new(2));
        assert_eq!(mapping.map(InternedU32::new(3)), InternedU32::new(3));
        assert_eq!(mapping.map(InternedU32::new(4)), InternedU32::new(4));
    }

    #[test]
    fn forward_mapping_compose() {
        let mapping1 = ForwardMapping(MappingImpl::<u32>::Map(Box::new([3, 1, 0, 4, 2])));
        let mapping2 = ForwardMapping(MappingImpl::<u32>::Map(Box::new([4, 0, 2, 3, 1])));
        let composed = mapping1.clone().compose(mapping2.clone());
        assert_eq!(
            composed,
            ForwardMapping(MappingImpl::Map(Box::new([3, 0, 4, 1, 2])))
        );

        for i in 0..5 {
            let index = InternedU32::new(i);
            assert_eq!(composed.map(index), mapping2.map(mapping1.map(index)));
        }
    }

    #[cfg(feature = "debug")]
    #[test]
    fn forward_mapping_count_remapped() {
        let mapping = ForwardMapping(MappingImpl::<u32>::Map(Box::new([3, 1, 0, 4, 2])));
        assert_eq!(mapping.count_remapped(), 4);
    }
}
