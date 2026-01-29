use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Helper trait for delta-encoding of interned values.
pub trait Accumulator: Default {
    /// Interned value type.
    type Value;
    /// Storage type of interned values.
    type Storage: Borrow<Self::Value>;
    /// Delta produced by this accumulator.
    type Delta;

    /// Helper function for serialization: accumulate the given value and
    /// returns the corresponding delta.
    fn fold(&mut self, v: &Self::Value) -> Self::Delta;

    /// Helper function for deserialization: accumulate the given delta and
    /// returns the corresponding value.
    fn unfold(&mut self, d: Self::Delta) -> Self::Storage;
}

/// Wrapper around an [`Arena`] that uses the given [`Accumulator`] to
/// serialize it with delta encoding.
#[derive(Default)]
pub struct DeltaEncoding<T, Accum> {
    inner: T,
    _phantom: PhantomData<Accum>,
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

    impl<T: ?Sized, Storage, Delta, Accum> Serialize for DeltaEncoding<Arena<T, Storage>, Accum>
    where
        Storage: Borrow<T>,
        Delta: Serialize,
        Accum: Accumulator<Value = T, Storage = Storage, Delta = Delta>,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut acc = Accum::default();
            serializer.collect_seq(self.vec.iter().map(|x| acc.fold(x.borrow())))
        }
    }

    impl<'de, T: ?Sized, Storage, Delta, Accum> Deserialize<'de>
        for DeltaEncoding<Arena<T, Storage>, Accum>
    where
        T: Eq + Hash,
        Storage: Borrow<T>,
        Delta: Deserialize<'de>,
        Accum: Accumulator<Value = T, Storage = Storage, Delta = Delta>,
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

    impl<'de, T: ?Sized, Storage, Delta, Accum> Visitor<'de> for DeltaArenaVisitor<T, Storage, Accum>
    where
        T: Eq + Hash,
        Storage: Borrow<T>,
        Delta: Deserialize<'de>,
        Accum: Accumulator<Value = T, Storage = Storage, Delta = Delta>,
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
            while let Some(delta) = seq.next_element()? {
                arena.push(acc.unfold(delta));
            }

            Ok(DeltaEncoding {
                inner: arena,
                _phantom: PhantomData,
            })
        }
    }
}
