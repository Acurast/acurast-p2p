# acurast-iroh

Processor side of the Acurast P2P layer, built on [iroh](https://www.iroh.computer/). One Rust
library shared by the Android and iOS processors.

| crate | what |
|---|---|
| `acurast-iroh` | The logic: iroh endpoint with the Acurast relays, endpoint registration on chain, role cache. Plain Rust, tested on the host. |
| `acurast-iroh-ffi` | [uniffi](https://mozilla.github.io/uniffi-rs/) bindings for Kotlin (Android AAR) and Swift (iOS xcframework). |

Platform-specific work (storing the iroh key, chain RPC, signing extrinsics with the processor
key) stays in the apps and is passed to the library as callbacks.

## Build and test

```sh
cargo build
cargo test
```

Generate the bindings from the host library:

```sh
cargo build -p acurast-iroh-ffi
cargo run -p acurast-iroh-ffi --features uniffi/cli --bin uniffi-bindgen -- \
  generate --library target/debug/libacurast_iroh_ffi.dylib --language kotlin --out-dir target/bindings
```
