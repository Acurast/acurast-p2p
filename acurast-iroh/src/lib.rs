//! Processor side of the Acurast P2P layer, built on iroh.
//!
//! Shared by the Android and iOS processors through `acurast-iroh-ffi`. Everything that depends on
//! the platform (key storage, chain access, extrinsic signing) is provided by the app.

pub use iroh::{EndpointId, SecretKey};
