use core::cmp::Ordering;
use core::fmt::Debug;
use core::hash::Hash;
use core::ops::{AddAssign, Sub};
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Interface to index values in an arena.
pub trait Index: Debug + Copy + Eq + Ord + Hash + AddAssign + Sub<Output = Self> {
    /// The minimum value of this type.
    const ZERO: Self;

    /// The maximum value of this type. This is used as a sentinel.
    const MAX: Self;

    /// Creates an index from the given value.
    fn from_usize(i: usize) -> Self;

    /// Converts this index to [`usize`].
    fn to_usize(&self) -> usize;

    /// Increments this index.
    fn incr(&mut self);

    /// Add two values, panicking on overflow.
    fn strict_add(self, other: Self) -> Self;
}

impl Index for u16 {
    const ZERO: Self = 0;
    const MAX: Self = u16::MAX;

    fn from_usize(i: usize) -> Self {
        i as u16
    }

    fn to_usize(&self) -> usize {
        *self as usize
    }

    fn incr(&mut self) {
        *self += 1;
    }

    fn strict_add(self, other: Self) -> Self {
        self.strict_add(other)
    }
}

impl Index for u32 {
    const ZERO: Self = 0;
    const MAX: Self = u32::MAX;

    fn from_usize(i: usize) -> Self {
        i as u32
    }

    fn to_usize(&self) -> usize {
        *self as usize
    }

    fn incr(&mut self) {
        *self += 1;
    }

    fn strict_add(self, other: Self) -> Self {
        self.strict_add(other)
    }
}

impl Index for u64 {
    const ZERO: Self = 0;
    const MAX: Self = u64::MAX;

    fn from_usize(i: usize) -> Self {
        i as u64
    }

    fn to_usize(&self) -> usize {
        *self as usize
    }

    fn incr(&mut self) {
        *self += 1;
    }

    fn strict_add(self, other: Self) -> Self {
        self.strict_add(other)
    }
}

macro_rules! impl_index {
    ( $typ:ty, $uint:ty, $bytes:expr ) => {
        impl Debug for $typ {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                self.to_uint().fmt(f)
            }
        }

        impl PartialOrd for $typ {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $typ {
            fn cmp(&self, other: &Self) -> Ordering {
                self.to_uint().cmp(&other.to_uint())
            }
        }

        impl Sub for $typ {
            type Output = Self;

            fn sub(self, other: Self) -> Self {
                Self::from_uint(self.to_uint() - other.to_uint())
            }
        }

        impl AddAssign for $typ {
            fn add_assign(&mut self, other: Self) {
                *self = Self::from_uint(self.to_uint() + other.to_uint())
            }
        }

        impl Index for $typ {
            const ZERO: Self = Self([0; $bytes]);
            const MAX: Self = Self([u8::MAX; $bytes]);

            fn from_usize(i: usize) -> Self {
                Self::from_uint(i as $uint)
            }

            fn to_usize(&self) -> usize {
                self.to_uint() as usize
            }

            fn incr(&mut self) {
                *self = Self::from_uint(self.to_uint() + 1);
            }

            fn strict_add(self, other: Self) -> Self {
                // The inner addition cannot overflow.
                Self::from_uint(self.to_uint() + other.to_uint())
            }
        }

        #[cfg(feature = "serde")]
        impl Serialize for $typ {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.to_uint().serialize(serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> Deserialize<'de> for $typ {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let i = Deserialize::deserialize(deserializer)?;
                Ok(Self::from_uint(i))
            }
        }
    };
}

/// A 24-bit index (3 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct U24([u8; 3]);

impl U24 {
    #[inline(always)]
    fn from_uint(x: u32) -> Self {
        if x >= 1 << 24 {
            panic!("Integer overflow while converting u32 to u24");
        }
        if cfg!(target_endian = "big") {
            let [_, a, b, c] = x.to_ne_bytes();
            Self([a, b, c])
        } else {
            let [a, b, c, _] = x.to_ne_bytes();
            Self([a, b, c])
        }
    }

    #[inline(always)]
    fn to_uint(self) -> u32 {
        let [a, b, c] = self.0;
        if cfg!(target_endian = "big") {
            u32::from_ne_bytes([0, a, b, c])
        } else {
            u32::from_ne_bytes([a, b, c, 0])
        }
    }
}

impl_index!(U24, u32, 3);

/// A 40-bit index (5 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct U40([u8; 5]);

impl U40 {
    #[inline(always)]
    fn from_uint(x: u64) -> Self {
        if x >= 1 << 40 {
            panic!("Integer overflow while converting u64 to u40");
        }
        if cfg!(target_endian = "big") {
            let [_, _, _, a, b, c, d, e] = x.to_ne_bytes();
            Self([a, b, c, d, e])
        } else {
            let [a, b, c, d, e, _, _, _] = x.to_ne_bytes();
            Self([a, b, c, d, e])
        }
    }

    #[inline(always)]
    fn to_uint(self) -> u64 {
        let [a, b, c, d, e] = self.0;
        if cfg!(target_endian = "big") {
            u64::from_ne_bytes([0, 0, 0, a, b, c, d, e])
        } else {
            u64::from_ne_bytes([a, b, c, d, e, 0, 0, 0])
        }
    }
}

impl_index!(U40, u64, 5);

/// A 48-bit index (6 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct U48([u8; 6]);

impl U48 {
    #[inline(always)]
    fn from_uint(x: u64) -> Self {
        if x >= 1 << 48 {
            panic!("Integer overflow while converting u64 to u48");
        }
        if cfg!(target_endian = "big") {
            let [_, _, a, b, c, d, e, f] = x.to_ne_bytes();
            Self([a, b, c, d, e, f])
        } else {
            let [a, b, c, d, e, f, _, _] = x.to_ne_bytes();
            Self([a, b, c, d, e, f])
        }
    }

    #[inline(always)]
    fn to_uint(self) -> u64 {
        let [a, b, c, d, e, f] = self.0;
        if cfg!(target_endian = "big") {
            u64::from_ne_bytes([0, 0, a, b, c, d, e, f])
        } else {
            u64::from_ne_bytes([a, b, c, d, e, f, 0, 0])
        }
    }
}

impl_index!(U48, u64, 6);

/// A 56-bit index (7 bytes).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct U56([u8; 7]);

impl U56 {
    #[inline(always)]
    fn from_uint(x: u64) -> Self {
        if x >= 1 << 56 {
            panic!("Integer overflow while converting u64 to u56");
        }
        if cfg!(target_endian = "big") {
            let [_, a, b, c, d, e, f, g] = x.to_ne_bytes();
            Self([a, b, c, d, e, f, g])
        } else {
            let [a, b, c, d, e, f, g, _] = x.to_ne_bytes();
            Self([a, b, c, d, e, f, g])
        }
    }

    #[inline(always)]
    fn to_uint(self) -> u64 {
        let [a, b, c, d, e, f, g] = self.0;
        if cfg!(target_endian = "big") {
            u64::from_ne_bytes([0, a, b, c, d, e, f, g])
        } else {
            u64::from_ne_bytes([a, b, c, d, e, f, g, 0])
        }
    }
}

impl_index!(U56, u64, 7);
