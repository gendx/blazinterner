#[cfg(feature = "get-size2")]
use get_size2::{GetSize, GetSizeTracker};
use std::borrow::Borrow;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Helper trait for delta-encoding of interned values.
pub trait Accumulator: Default {
    /// Interned value type.
    type Value: ?Sized;
    /// Storage type of interned values.
    type Storage: Borrow<Self::Value>;
    /// Delta produced by this accumulator.
    type Delta: ?Sized;
    /// Storage type of the delta produced by this accumulator.
    type DeltaStorage: Borrow<Self::Delta>;

    /// Helper function for serialization: accumulate the given value and
    /// returns the corresponding delta.
    fn fold(&mut self, v: &Self::Value) -> Self::DeltaStorage;

    /// Helper function for deserialization: accumulate the given delta and
    /// returns the corresponding value.
    fn unfold(&mut self, d: &Self::Delta) -> Self::Storage;
}

/// Wrapper around an [`Arena`](crate::Arena) or
/// [`ArenaSlice`](crate::ArenaSlice) that uses the given [`Accumulator`] to
/// serialize it with delta encoding.
#[derive(Default)]
pub struct DeltaEncoding<T, Accum> {
    pub(crate) inner: T,
    pub(crate) _phantom: PhantomData<Accum>,
}

impl<T, Accum> DeltaEncoding<T, Accum> {
    /// Creates a new wrapper around the given arena.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Extracts the inner arena from this wrapper.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Converts from `&DeltaEncoding<T, Accum>` to `DeltaEncoding<&T, Accum>`
    pub fn as_ref(&self) -> DeltaEncoding<&T, Accum> {
        DeltaEncoding {
            inner: &self.inner,
            _phantom: PhantomData,
        }
    }
}

impl<T, Accum> Deref for DeltaEncoding<T, Accum> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, Accum> DerefMut for DeltaEncoding<T, Accum> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T, Accum> Debug for DeltaEncoding<T, Accum>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T, Accum> PartialEq for DeltaEncoding<T, Accum>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T, Accum> Eq for DeltaEncoding<T, Accum> where T: Eq {}

#[cfg(feature = "get-size2")]
impl<T, Accum> GetSize for DeltaEncoding<T, Accum>
where
    T: GetSize,
{
    fn get_stack_size() -> usize {
        T::get_stack_size()
    }

    fn get_heap_size(&self) -> usize {
        self.inner.get_heap_size()
    }

    fn get_heap_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.inner.get_heap_size_with_tracker(tracker)
    }

    fn get_size(&self) -> usize {
        self.inner.get_size()
    }

    fn get_size_with_tracker<Tr: GetSizeTracker>(&self, tracker: Tr) -> (usize, Tr) {
        self.inner.get_size_with_tracker(tracker)
    }
}

#[cfg(feature = "serde")]
mod serialization {
    use super::*;
    use crate::Arena;
    use appendvec::AppendVec;
    use dashtable::DashTable;
    use hashbrown::DefaultHashBuilder;
    use serde::de::{SeqAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::hash::Hash;
    #[cfg(feature = "debug")]
    use std::sync::atomic::AtomicUsize;

    impl<T: ?Sized, Storage, DeltaStorage, Accum> Serialize for DeltaEncoding<&Arena<T, Storage>, Accum>
    where
        Storage: Borrow<T>,
        DeltaStorage: Serialize,
        Accum: Accumulator<Value = T, Storage = Storage, DeltaStorage = DeltaStorage>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut acc = Accum::default();
            serializer.collect_seq(self.vec.iter().map(|x| acc.fold(x.borrow())))
        }
    }

    impl<'de, T: ?Sized, Storage, Delta, DeltaStorage, Accum> Deserialize<'de>
        for DeltaEncoding<Arena<T, Storage>, Accum>
    where
        T: Eq + Hash,
        Storage: Borrow<T>,
        DeltaStorage: Borrow<Delta> + Deserialize<'de>,
        Accum:
            Accumulator<Value = T, Storage = Storage, Delta = Delta, DeltaStorage = DeltaStorage>,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(DeltaArenaVisitor::new())
        }
    }

    struct DeltaArenaVisitor<T: ?Sized, Storage, Accum> {
        _phantom: PhantomData<fn() -> Arena<T, Storage>>,
        _accum: PhantomData<Accum>,
    }

    impl<T: ?Sized, Storage, Accum> DeltaArenaVisitor<T, Storage, Accum> {
        fn new() -> Self {
            Self {
                _phantom: PhantomData,
                _accum: PhantomData,
            }
        }
    }

    impl<'de, T: ?Sized, Storage, Delta, DeltaStorage, Accum> Visitor<'de>
        for DeltaArenaVisitor<T, Storage, Accum>
    where
        T: Eq + Hash,
        Storage: Borrow<T>,
        DeltaStorage: Borrow<Delta> + Deserialize<'de>,
        Accum:
            Accumulator<Value = T, Storage = Storage, Delta = Delta, DeltaStorage = DeltaStorage>,
    {
        type Value = DeltaEncoding<Arena<T, Storage>, Accum>;

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

            let mut acc = Accum::default();
            while let Some(delta) = seq.next_element::<DeltaStorage>()? {
                arena.push(acc.unfold(delta.borrow()));
            }

            Ok(DeltaEncoding {
                inner: arena,
                _phantom: PhantomData,
            })
        }
    }
}
