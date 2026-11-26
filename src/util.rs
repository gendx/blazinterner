use crate::Index;
#[cfg(not(feature = "sync"))]
use alloc::string::String;
use alloc::vec::Vec;
#[cfg(feature = "sync")]
use appendvec::AppendStr;
#[cfg(feature = "sync")]
use appendvec::AppendVec;
use core::marker::PhantomData;
use core::ops::Range;
#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};

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

impl<I: Index> CopyRange<I> {
    fn from_usize(range: Range<usize>) -> Self {
        assert!(range.start <= I::MAX.to_usize());
        assert!(range.end <= I::MAX.to_usize());
        Self {
            start: I::from_usize(range.start),
            end: I::from_usize(range.end),
        }
    }
}

pub struct IndexedVec<T, I> {
    #[cfg(not(feature = "sync"))]
    vec: Vec<T>,
    #[cfg(feature = "sync")]
    vec: AppendVec<T>,
    _phantom: PhantomData<fn(I) -> T>,
}

#[cfg(feature = "get-size2")]
impl<T, I> GetSize for IndexedVec<T, I>
where
    T: GetSize,
{
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.vec.get_heap_size_with_tracker(tracker)
    }
}

impl<T, I: Index> core::ops::Index<I> for IndexedVec<T, I> {
    type Output = T;

    fn index(&self, i: I) -> &Self::Output {
        &self.vec[i.to_usize()]
    }
}

impl<T, I: Index> core::ops::Index<CopyRange<I>> for IndexedVec<T, I> {
    type Output = [T];

    fn index(&self, range: CopyRange<I>) -> &Self::Output {
        let range = range.start.to_usize()..range.end.to_usize();
        &self.vec[range]
    }
}

impl<T, I> IndexedVec<T, I> {
    pub fn new() -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            vec: Vec::new(),
            #[cfg(feature = "sync")]
            vec: AppendVec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        self.vec.iter()
    }
}

impl<T, I: Index> IndexedVec<T, I> {
    pub fn try_with_capacity(len: usize) -> Option<Self> {
        if len != 0 && len - 1 > I::MAX.to_usize() {
            return None;
        }
        Self::try_with_capacity_(len)
    }

    pub fn try_with_capacity_slices(len: usize) -> Option<Self> {
        if len > I::MAX.to_usize() {
            return None;
        }
        Self::try_with_capacity_(len)
    }

    fn try_with_capacity_(len: usize) -> Option<Self> {
        Some(Self {
            #[cfg(not(feature = "sync"))]
            vec: Vec::with_capacity(len),
            #[cfg(feature = "sync")]
            vec: AppendVec::try_with_capacity(len)?,
            _phantom: PhantomData,
        })
    }

    pub fn get(&self, i: I) -> Option<&T> {
        let i = i.to_usize();
        #[cfg(not(feature = "sync"))]
        let t = self.vec.get(i)?;
        #[cfg(feature = "sync")]
        let t = self.vec.try_index(i)?;
        Some(t)
    }

    #[cfg(feature = "sync")]
    pub fn try_push(&self, value: T) -> Option<I> {
        let id = self.vec.try_push(value, I::MAX.to_usize())?;
        debug_assert!(id <= I::MAX.to_usize());
        Some(I::from_usize(id))
    }

    #[cfg(not(feature = "sync"))]
    pub fn try_push_mut(&mut self, value: T) -> Option<I> {
        let id = self.vec.len();
        if id > I::MAX.to_usize() {
            return None;
        }
        self.vec.push(value);
        Some(I::from_usize(id))
    }

    #[cfg(feature = "sync")]
    pub fn try_push_mut(&mut self, value: T) -> Option<I> {
        let id = self.vec.try_push_mut(value, I::MAX.to_usize())?;
        debug_assert!(id <= I::MAX.to_usize());
        Some(I::from_usize(id))
    }

    pub fn truncate(&mut self, len: usize) {
        debug_assert!(len == 0 || len - 1 <= I::MAX.to_usize());
        self.vec.truncate(len);
    }

    #[cfg(not(feature = "sync"))]
    #[inline(always)]
    fn check_slice_limit(start: usize, len: usize) -> Option<()> {
        let limit = I::MAX.to_usize();
        if len > limit || start > limit - len {
            None
        } else {
            Some(())
        }
    }

    /// # Safety
    ///
    /// This function requires the iterator length to be correct. This is akin to the nightly-only [`TrustedLen`](core::iter::TrustedLen) trait.
    #[cfg(not(feature = "sync"))]
    pub unsafe fn try_extend(
        &mut self,
        value: impl ExactSizeIterator<Item = T>,
    ) -> Option<CopyRange<I>> {
        let start = self.vec.len();
        Self::check_slice_limit(start, value.len())?;
        self.vec.extend(value);
        let end = self.vec.len();
        Some(CopyRange::from_usize(start..end))
    }

    #[cfg(not(feature = "sync"))]
    pub fn try_extend_array<const N: usize>(&mut self, value: [T; N]) -> Option<CopyRange<I>> {
        let start = self.vec.len();
        Self::check_slice_limit(start, N)?;
        self.vec.extend(value);
        let end = self.vec.len();
        Some(CopyRange::from_usize(start..end))
    }

    #[cfg(not(feature = "sync"))]
    pub fn try_append(&mut self, value: &mut Vec<T>) -> Option<CopyRange<I>> {
        let start = self.vec.len();
        Self::check_slice_limit(start, value.len())?;
        self.vec.append(value);
        let end = self.vec.len();
        Some(CopyRange::from_usize(start..end))
    }
}

#[cfg(not(feature = "sync"))]
impl<T: Clone, I: Index> IndexedVec<T, I> {
    pub fn try_extend_from_slice(&mut self, value: &[T]) -> Option<CopyRange<I>> {
        let start = self.vec.len();
        Self::check_slice_limit(start, value.len())?;
        self.vec.extend_from_slice(value);
        let end = self.vec.len();
        Some(CopyRange::from_usize(start..end))
    }
}

#[cfg(feature = "sync")]
impl<T: Default, I: Index> IndexedVec<T, I> {
    pub fn try_push_owned_slice(&self, value: Vec<T>) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_owned_slice(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    pub fn try_push_owned_slice_mut(&mut self, value: Vec<T>) -> Option<CopyRange<I>> {
        let range = self
            .vec
            .try_push_owned_slice_mut(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    pub fn try_push_array<const N: usize>(&self, value: [T; N]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_array(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    pub fn try_push_array_mut<const N: usize>(&mut self, value: [T; N]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_array_mut(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    /// # Safety
    ///
    /// This function requires the iterator length to be correct. This is akin to the nightly-only [`TrustedLen`](core::iter::TrustedLen) trait.
    pub unsafe fn try_push_contiguous(
        &self,
        value: impl ExactSizeIterator<Item = T>,
    ) -> Option<CopyRange<I>> {
        // SAFETY: The caller ensures that the iterator length is correct.
        let range = unsafe { self.vec.try_push_contiguous(value, I::MAX.to_usize()) }?;
        Some(CopyRange::from_usize(range))
    }

    /// # Safety
    ///
    /// This function requires the iterator length to be correct. This is akin to the nightly-only [`TrustedLen`](core::iter::TrustedLen) trait.
    pub unsafe fn try_push_contiguous_mut(
        &mut self,
        value: impl ExactSizeIterator<Item = T>,
    ) -> Option<CopyRange<I>> {
        // SAFETY: The caller ensures that the iterator length is correct.
        let range = unsafe { self.vec.try_push_contiguous_mut(value, I::MAX.to_usize()) }?;
        Some(CopyRange::from_usize(range))
    }
}

#[cfg(feature = "sync")]
impl<T: Default + Clone, I: Index> IndexedVec<T, I> {
    pub fn try_push_slice(&self, value: &[T]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_slice(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    pub fn try_push_slice_mut(&mut self, value: &[T]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_slice_mut(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }
}

#[cfg(feature = "sync")]
impl<T: Default + Copy, I: Index> IndexedVec<T, I> {
    pub fn try_push_slice_copy(&self, value: &[T]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_slice_copy(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    pub fn try_push_slice_copy_mut(&mut self, value: &[T]) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_slice_copy_mut(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }
}

pub struct IndexedStr<I> {
    #[cfg(not(feature = "sync"))]
    vec: String,
    #[cfg(feature = "sync")]
    vec: AppendStr,
    _phantom: PhantomData<fn(I) -> u8>,
}

#[cfg(feature = "get-size2")]
impl<I> GetSize for IndexedStr<I> {
    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.vec.get_heap_size_with_tracker(tracker)
    }
}

impl<I: Index> core::ops::Index<CopyRange<I>> for IndexedStr<I> {
    type Output = str;

    fn index(&self, range: CopyRange<I>) -> &Self::Output {
        let range = range.start.to_usize()..range.end.to_usize();
        &self.vec[range]
    }
}

impl<I> IndexedStr<I> {
    pub fn new() -> Self {
        Self {
            #[cfg(not(feature = "sync"))]
            vec: String::new(),
            #[cfg(feature = "sync")]
            vec: AppendStr::new(),
            _phantom: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }
}

impl<I: Index> IndexedStr<I> {
    pub fn try_with_capacity_slices(len: usize) -> Option<Self> {
        if len > I::MAX.to_usize() {
            return None;
        }
        Some(Self {
            #[cfg(not(feature = "sync"))]
            vec: String::with_capacity(len),
            #[cfg(feature = "sync")]
            vec: AppendStr::try_with_capacity(len)?,
            _phantom: PhantomData,
        })
    }

    pub fn get_bytes(&self, range: CopyRange<I>) -> &[u8] {
        let range = range.start.to_usize()..range.end.to_usize();
        #[cfg(not(feature = "sync"))]
        return &self.vec.as_bytes()[range];
        #[cfg(feature = "sync")]
        return self.vec.get_bytes(range);
    }

    pub fn truncate(&mut self, len: usize) {
        debug_assert!(len == 0 || len - 1 <= I::MAX.to_usize());
        self.vec.truncate(len);
    }

    #[cfg(not(feature = "sync"))]
    #[inline(always)]
    fn check_slice_limit(start: usize, len: usize) -> Option<()> {
        let limit = I::MAX.to_usize();
        if len > limit || start > limit - len {
            None
        } else {
            Some(())
        }
    }

    #[cfg(feature = "sync")]
    pub fn try_push_str(&self, value: &str) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_str(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }

    #[cfg(not(feature = "sync"))]
    pub fn try_push_str_mut(&mut self, value: &str) -> Option<CopyRange<I>> {
        let start = self.vec.len();
        Self::check_slice_limit(start, value.len())?;
        self.vec.push_str(value);
        let end = self.vec.len();
        Some(CopyRange::from_usize(start..end))
    }

    #[cfg(feature = "sync")]
    pub fn try_push_str_mut(&mut self, value: &str) -> Option<CopyRange<I>> {
        let range = self.vec.try_push_str_mut(value, I::MAX.to_usize())?;
        Some(CopyRange::from_usize(range))
    }
}
