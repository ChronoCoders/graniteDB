#[cfg(feature = "loom")]
pub use loom::sync::{Arc, Mutex};

#[cfg(not(feature = "loom"))]
pub use std::sync::{Arc, Mutex};

#[cfg(feature = "loom")]
pub use loom::thread;

#[cfg(not(feature = "loom"))]
pub use std::thread;
