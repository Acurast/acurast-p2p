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

## Packages for the apps

| platform | command | output |
|---|---|---|
| iOS | `make ios` (needs [`cargo-swift`](https://github.com/antoniusnaumann/cargo-swift) 0.9.0: `make install-cargo-swift`) | Swift package `AcurastIroh/` with `AcurastIrohFFI.xcframework` (not committed), added to the app as a local package like `AcurastSDK` |
| Android | `make android` (NDK 29.0.14206865, JDK 17; `android/local.properties` with `sdk.dir`) | `android/lib/build/outputs/aar/lib-release.aar`, published as `com.acurast:acurast-iroh` (`./gradlew :lib:publishToMavenLocal` locally) |

`libc` is pinned to 0.2.186 in `Cargo.lock`: later versions no longer define `in6_ifreq` on iOS,
which `netdev` (via iroh) needs. The iOS minimum is 13.0, as for `AcurastSDK`.

## Try it against a relay

`p2p-echo` reaches one endpoint from another through a relay, knowing only its `EndpointId`. The
Acurast relays admit only endpoints registered on chain, so register both keys first (or allowlist
them on the node with `--p2p-relay-allow`). The local network with a relay node is described in
`acurast-substrate/zombienet/README.md`.

```sh
cargo run -p acurast-iroh --example p2p-echo -- listen http://127.0.0.1:3340 --secret <hex>
cargo run -p acurast-iroh --example p2p-echo -- dial http://127.0.0.1:3340 <endpoint-id> --secret <hex>
```

Each run prints its secret and endpoint id; without `--secret` a new key is generated.
