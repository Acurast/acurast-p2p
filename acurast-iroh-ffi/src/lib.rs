//! uniffi bindings of `acurast-iroh`, consumed by the Android (AAR) and iOS (xcframework)
//! processors.

uniffi::setup_scaffolding!();

/// Version of this library, so the app can log which one it runs.
#[uniffi::export]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
