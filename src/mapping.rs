use super::{Arena, ArenaSlice, ArenaStr, Interned, InternedSlice, InternedStr};
#[cfg(feature = "retain")]
use bit_set::BitSet;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::Hash;

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Ord,
    Storage: Borrow<T>,
{
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping {
        let mut mapping: Vec<u32> = (0..self.len() as u32).collect();
        mapping.sort_by_cached_key(|i| self.lookup_ref(Interned::new(*i)));
        let reverse = ReverseMapping(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage> {
    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(Interned<T, Storage>) -> bool) -> Mapping {
        Mapping::retain(self.len(), |i| filter(Interned::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    #[cfg(feature = "retain")]
    pub fn retain_values(&self, values: impl Iterator<Item = Interned<T, Storage>>) -> Mapping {
        let len = self.len();
        let mut retained_ids = BitSet::with_capacity(len);
        for v in values {
            retained_ids.insert(v.id_() as usize);
        }
        Mapping::retain(len, |i| retained_ids.contains(i as usize))
    }
}

impl<T: ?Sized, Storage> Arena<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Clone,
{
    /// Returns a re-ordered version of this arena based on the given mapping.
    pub fn map(&self, mapping: &ReverseMapping) -> Self {
        let mut arena = Arena::with_capacity(mapping.len());
        for i in mapping.iter() {
            arena.push(self.lookup(Interned::new(i)));
        }
        arena
    }
}

impl<T> ArenaSlice<T>
where
    T: Ord,
{
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping {
        let mut mapping: Vec<u32> = (0..self.slices() as u32).collect();
        mapping.sort_by_cached_key(|i| CustomSliceOrd(self.lookup(InternedSlice::new(*i))));
        let reverse = ReverseMapping(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }
}

impl<T> ArenaSlice<T> {
    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(InternedSlice<T>) -> bool) -> Mapping {
        Mapping::retain(self.slices(), |i| filter(InternedSlice::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    #[cfg(feature = "retain")]
    pub fn retain_values(&self, values: impl Iterator<Item = InternedSlice<T>>) -> Mapping {
        let len = self.slices();
        let mut retained_ids = BitSet::with_capacity(len);
        for v in values {
            retained_ids.insert(v.id_() as usize);
        }
        Mapping::retain(len, |i| retained_ids.contains(i as usize))
    }
}

impl<T> ArenaSlice<T>
where
    T: Default + Clone + Eq + Hash,
{
    /// Returns a re-ordered version of this arena based on the given mapping.
    pub fn map(&self, mapping: &ReverseMapping) -> Self {
        let mut arena = ArenaSlice::with_capacity(mapping.len(), self.items());
        for i in mapping.iter() {
            arena.push(self.lookup(InternedSlice::new(i)));
        }
        arena
    }
}

impl<T> ArenaSlice<T>
where
    T: Default + Eq + Hash,
{
    /// Returns a re-ordered version of this arena based on the given mapping,
    /// where each slice element is additionally transformed according to
    /// the given function `f`.
    pub fn map2(&self, mapping: &ReverseMapping, f: impl Fn(&T) -> T) -> Self {
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

impl ArenaStr {
    /// Returns a sorted mapping of the items in this arena.
    pub fn sort(&self) -> Mapping {
        let mut mapping: Vec<u32> = (0..self.strings() as u32).collect();
        mapping.sort_by_cached_key(|i| CustomStrOrd(self.lookup(InternedStr::new(*i))));
        let reverse = ReverseMapping(mapping.into_boxed_slice());
        let forward = reverse.reverse();
        Mapping { forward, reverse }
    }

    /// Returns a mapping to build an arena containing only the items of this
    /// arena that satisfy the given predicate.
    pub fn retain(&self, filter: impl Fn(InternedStr) -> bool) -> Mapping {
        Mapping::retain(self.strings(), |i| filter(InternedStr::new(i)))
    }

    /// Returns a mapping to build an arena containing only the given items.
    #[cfg(feature = "retain")]
    pub fn retain_values(&self, values: impl Iterator<Item = InternedStr>) -> Mapping {
        let len = self.strings();
        let mut retained_ids = BitSet::with_capacity(len);
        for v in values {
            retained_ids.insert(v.id_() as usize);
        }
        Mapping::retain(len, |i| retained_ids.contains(i as usize))
    }

    /// Returns a re-ordered version of this arena based on the given mapping.
    pub fn map(&self, mapping: &ReverseMapping) -> Self {
        let mut arena = ArenaStr::with_capacity(mapping.len(), self.bytes());
        for i in mapping.iter() {
            arena.push(self.lookup(InternedStr::new(i)));
        }
        arena
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
pub struct Mapping {
    /// Forward mapping, to map interned handles.
    pub forward: ForwardMapping,
    /// Reverse mapping, to re-order the arena.
    pub reverse: ReverseMapping,
}

impl Mapping {
    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.forward.is_identity()
    }

    fn retain(len: usize, filter: impl Fn(u32) -> bool) -> Self {
        let mut reverse = Vec::new();
        let mut forward = Vec::with_capacity(len);
        let mut next = 0;
        let mut is_identity = true;

        for i in 0..len as u32 {
            if filter(i) {
                reverse.push(i);
                forward.push(next);
                next += 1;
            } else {
                forward.push(u32::MAX);
                is_identity = false;
            }
        }

        let reverse = ReverseMapping(reverse.into_boxed_slice());
        let forward = if is_identity {
            MappingImpl::Identity(len as u32)
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
pub struct ReverseMapping(Box<[u32]>);

impl ReverseMapping {
    /// Returns the number of mapped items.
    fn len(&self) -> usize {
        self.0.len()
    }

    fn reverse(&self) -> ForwardMapping {
        if self.is_identity() {
            ForwardMapping(MappingImpl::Identity(self.0.len() as u32))
        } else {
            let mut reverse = vec![0; self.0.len()];
            for i in 0..self.0.len() as u32 {
                reverse[self.0[i as usize] as usize] = i;
            }
            ForwardMapping(MappingImpl::Map(reverse.into_boxed_slice()))
        }
    }

    /// Checks wether this mapping is the identity.
    fn is_identity(&self) -> bool {
        self.0.iter().enumerate().all(|(i, j)| i == *j as usize)
    }

    /// Returns the mapped indices in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = u32> {
        self.0.iter().copied()
    }
}

/// A mapping to re-order items in an [`Arena`], [`ArenaSlice`] or [`ArenaStr`].
#[cfg_attr(test, derive(Clone, PartialEq, Eq, Debug))]
pub struct ForwardMapping(MappingImpl);

impl ForwardMapping {
    /// Creates a new identity mapping with the given number of items.
    pub fn identity(count: u32) -> Self {
        Self(MappingImpl::Identity(count))
    }

    /// Checks wether this mapping is the identity.
    pub fn is_identity(&self) -> bool {
        self.0.is_identity()
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    pub fn map<T: ?Sized, Storage>(&self, index: Interned<T, Storage>) -> Interned<T, Storage> {
        Interned::new(self.0.at(index.id_()))
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    pub fn map_slice<T>(&self, index: InternedSlice<T>) -> InternedSlice<T> {
        InternedSlice::new(self.0.at(index.id_()))
    }

    /// Maps the given interned handle to a handle for the re-ordered arena.
    pub fn map_str(&self, index: InternedStr) -> InternedStr {
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
enum MappingImpl {
    Identity(u32),
    Map(Box<[u32]>),
}

impl MappingImpl {
    /// Checks wether this mapping is the identity.
    fn is_identity(&self) -> bool {
        match self {
            Self::Identity(_) => true,
            Self::Map(_) => false,
        }
    }

    fn len(&self) -> u32 {
        match self {
            Self::Identity(len) => *len,
            Self::Map(map) => map.len() as u32,
        }
    }

    fn at(&self, index: u32) -> u32 {
        match self {
            MappingImpl::Identity(_) => index,
            MappingImpl::Map(map) => map[index as usize],
        }
    }

    fn compose(self, other: MappingImpl) -> Self {
        assert_eq!(self.len(), other.len());
        match (self, other) {
            (MappingImpl::Identity(len), MappingImpl::Identity(_)) => MappingImpl::Identity(len),
            (MappingImpl::Map(map), MappingImpl::Identity(_))
            | (MappingImpl::Identity(_), MappingImpl::Map(map)) => MappingImpl::Map(map),
            (MappingImpl::Map(left), MappingImpl::Map(right)) => {
                MappingImpl::Map(left.iter().map(|i| right[*i as usize]).collect())
            }
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
                .filter(|&(i, j)| i != *j as usize)
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
        let mut arena = ArenaStr::default();
        arena.push("bbbb");
        arena.push("dd");
        arena.push("e");
        arena.push("aaaaa");
        arena.push("ccc");

        let mapping = arena.sort();
        assert!(!mapping.is_identity());
        assert_eq!(
            mapping.reverse,
            ReverseMapping(vec![2, 1, 4, 0, 3].into_boxed_slice())
        );
        assert_eq!(
            mapping.forward,
            ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()))
        );
    }

    #[test]
    fn arena_str_sort_identity() {
        let mut arena = ArenaStr::default();
        arena.push("e");
        arena.push("dd");
        arena.push("ccc");
        arena.push("bbbb");
        arena.push("aaaaa");

        let mapping = arena.sort();
        assert!(mapping.is_identity());
        assert_eq!(
            mapping.reverse,
            ReverseMapping(vec![0, 1, 2, 3, 4].into_boxed_slice())
        );
        assert_eq!(mapping.forward, ForwardMapping(MappingImpl::Identity(5)));
    }

    #[test]
    fn arena_str_sort_map() {
        let mut arena = ArenaStr::default();
        arena.push("bbbb");
        arena.push("dd");
        arena.push("e");
        arena.push("aaaaa");
        arena.push("ccc");

        let mapping = arena.sort();
        let sorted = arena.map(&mapping.reverse);

        let mut expected = ArenaStr::default();
        expected.push("e");
        expected.push("dd");
        expected.push("ccc");
        expected.push("bbbb");
        expected.push("aaaaa");

        assert_eq!(sorted, expected);
    }

    #[test]
    fn arena_str_retain_map() {
        let mut arena = ArenaStr::default();
        let _ = arena.intern_mut("bbbb");
        let d = arena.intern_mut("dd");
        let e = arena.intern_mut("e");
        let _ = arena.intern_mut("aaaaa");
        let c = arena.intern_mut("ccc");

        let mapping = arena.retain(|i| arena.lookup(i).len() <= 3);
        let filtered = arena.map(&mapping.reverse);

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
    fn arena_str_retain_values_map() {
        let mut arena = ArenaStr::default();
        let _ = arena.intern_mut("bbbb");
        let d = arena.intern_mut("dd");
        let e = arena.intern_mut("e");
        let _ = arena.intern_mut("aaaaa");
        let c = arena.intern_mut("ccc");

        let mapping = arena.retain_values([d, e, c].into_iter());
        let filtered = arena.map(&mapping.reverse);

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
        let mapping = ReverseMapping(vec![2, 1, 4, 0, 3].into_boxed_slice());
        assert_eq!(mapping.iter().collect::<Vec<_>>(), vec![2, 1, 4, 0, 3]);
    }

    #[test]
    fn reverse_mapping_reverse() {
        let mapping = ReverseMapping(vec![2, 1, 4, 0, 3].into_boxed_slice());
        assert_eq!(
            mapping.reverse(),
            ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()))
        );

        let mapping = ReverseMapping(vec![0, 1, 2, 3, 4].into_boxed_slice());
        assert_eq!(mapping.reverse(), ForwardMapping(MappingImpl::Identity(5)));
    }

    #[test]
    fn forward_mapping_map() {
        let mapping = ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()));
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
        let mapping1 = ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()));
        let mapping2 = ForwardMapping(MappingImpl::Map(vec![4, 0, 2, 3, 1].into_boxed_slice()));
        let composed = mapping1.clone().compose(mapping2.clone());
        assert_eq!(
            composed,
            ForwardMapping(MappingImpl::Map(vec![3, 0, 4, 1, 2].into_boxed_slice()))
        );

        for i in 0..5 {
            let index = InternedU32::new(i);
            assert_eq!(composed.map(index), mapping2.map(mapping1.map(index)));
        }
    }

    #[cfg(feature = "debug")]
    #[test]
    fn forward_mapping_count_remapped() {
        let mapping = ForwardMapping(MappingImpl::Map(vec![3, 1, 0, 4, 2].into_boxed_slice()));
        assert_eq!(mapping.count_remapped(), 4);
    }
}
