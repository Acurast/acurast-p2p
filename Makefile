IOS_RUST_TARGETS := aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
# Same minimum as the AcurastSDK package; older targets lack symbols the Rust std links to.
export IPHONEOS_DEPLOYMENT_TARGET := 13.0

install-cargo-swift:
	cargo install cargo-swift@0.9.0 -f

ensure-ios-targets:
	rustup target add $(IOS_RUST_TARGETS)

# Swift package AcurastP2P (xcframework AcurastP2PFFI + Swift bindings) at the repo root.
ios: ensure-ios-targets
	cd acurast-p2p-ffi && cargo swift package --platforms ios --name AcurastP2P --xcframework-name AcurastP2PFFI --release --accept-all
	rm -rf AcurastP2P && mv acurast-p2p-ffi/AcurastP2P AcurastP2P

# Android library (AAR) in android/lib/build/outputs/aar.
android:
	cd android && ./gradlew :lib:assembleRelease

.PHONY: install-cargo-swift ensure-ios-targets ios android
