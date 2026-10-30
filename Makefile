IOS_RUST_TARGETS := aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
# Same minimum as the AcurastSDK package; older targets lack symbols the Rust std links to.
export IPHONEOS_DEPLOYMENT_TARGET := 13.0

install-cargo-swift:
	cargo install cargo-swift@0.9.0 -f

ensure-ios-targets:
	rustup target add $(IOS_RUST_TARGETS)

# Swift package AcurastIroh (xcframework AcurastIrohFFI + Swift bindings) at the repo root.
ios: ensure-ios-targets
	cd acurast-iroh-ffi && cargo swift package --platforms ios --name AcurastIroh --xcframework-name AcurastIrohFFI --release --accept-all
	rm -rf AcurastIroh && mv acurast-iroh-ffi/AcurastIroh AcurastIroh

# Android library (AAR) in android/lib/build/outputs/aar.
android:
	cd android && ./gradlew :lib:assembleRelease

.PHONY: install-cargo-swift ensure-ios-targets ios android
