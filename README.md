# acurast-p2p

Processor side of the Acurast P2P layer, currently built on [iroh](https://www.iroh.computer/).
One Rust library shared by the Android and iOS processors. It replaces `acup2p` (libp2p, Android
only).

The name does not mention the network library on purpose: the apps only see `P2pNode` and the
callbacks, so the library underneath can change without renaming anything.

| crate | what |
|---|---|
| `acurast-p2p` | The logic: iroh endpoint with the Acurast relays, endpoint registration on chain, address publishing and lookup through the relay nodes' registry, role cache. Plain Rust, tested on the host. |
| `acurast-p2p-cli` | `acurast-p2p` command for managers: register a P2P key, list processors, send them commands. |
| `acurast-p2p-ffi` | [uniffi](https://mozilla.github.io/uniffi-rs/) bindings for Kotlin (Android AAR) and Swift (iOS xcframework). |

Platform-specific work (storing the iroh key, chain RPC, signing extrinsics with the processor
key) stays in the apps and is passed to the library as callbacks.

## Build and test

```sh
cargo build
cargo test
```

Generate the bindings from the host library:

```sh
cargo build -p acurast-p2p-ffi
cargo run -p acurast-p2p-ffi --features uniffi/cli --bin uniffi-bindgen -- \
  generate --library target/debug/libacurast_p2p_ffi.dylib --language kotlin --out-dir target/bindings
```

## Packages for the apps

| platform | command | output |
|---|---|---|
| iOS | `make ios` (needs [`cargo-swift`](https://github.com/antoniusnaumann/cargo-swift) 0.9.0: `make install-cargo-swift`) | Swift package `AcurastP2P/` with `AcurastP2PFFI.xcframework` (not committed), added to the app as a local package like `AcurastSDK` |
| Android | `make android` (NDK 29.0.14206865, JDK 17; `android/local.properties` with `sdk.dir`) | `android/lib/build/outputs/aar/lib-release.aar`, published as `com.acurast:acurast-p2p` (`./gradlew :lib:publishToMavenLocal` locally) |

`libc` is pinned to 0.2.186 in `Cargo.lock`: later versions no longer define `in6_ifreq` on iOS,
which `netdev` (via iroh) needs. The iOS minimum is 13.0, as for `AcurastSDK`.

## Manage processors from the command line

Managers send commands to their own processors over the P2P layer with `acurast-p2p`. It uses its
own P2P key, stored in `~/.config/acurast-p2p/manager.key` (or `--key`), and never the manager's
account key: the one on-chain step, registering the P2P key, is signed in the manager's wallet.

```sh
cargo install --path acurast-p2p-cli
export ACURAST_RPC=wss://<node of the network>
export ACURAST_MANAGER=<manager address>

acurast-p2p init        # create the P2P key (once)
acurast-p2p register    # prints the call (and a polkadot.js link) to sign with the manager's account
acurast-p2p processors  # the manager's processors and their P2P endpoints
acurast-p2p status <processor> | --all
acurast-p2p reboot <processor> | --all [--force]
acurast-p2p restart-app <processor> | --all [--force]
```

`register` reads the genesis hash and the call index from the chain, and says when the key is
already registered. In polkadot.js apps the call goes to Developer > Extrinsics > Decode, then
Submit. Registering costs the transaction fee, plus the endpoint deposit if the network sets one;
a manager can register more than one key (one per computer).

Processors refuse commands from any other key (`Forbidden`), commands older than a minute
(`Stale`, so the computer's clock must be right) and, without `--force`, `reboot` and
`restart-app` while a job runs (`Busy`). `reboot` needs device owner rights on Android and is not
supported on iOS. A processor that is offline, asleep or with the app in the background on iOS
does not reply. `reboot` and `restart-app` with `--all` ask for confirmation (`-y` skips it).

## Try it against a relay

`p2p-echo` reaches one endpoint from another through the relays, knowing only its `EndpointId`:
the listener publishes its address to the registry of the relay nodes, where the dialer finds it.
Relays are given as on chain, `<relay-id>@<relay-url>` (comma-separated); a relay node logs its
endpoint id at start. The Acurast relays admit only endpoints registered on chain, so register
both keys first (or allowlist them on the node with `--p2p-relay-allow`). The local network with
relay nodes is described in `acurast-substrate/zombienet/README.md`.

```sh
cargo run -p acurast-p2p --example p2p-echo -- listen <relay-id>@http://127.0.0.1:3340 --secret <hex>
cargo run -p acurast-p2p --example p2p-echo -- dial <relay-id>@http://127.0.0.1:3340 <endpoint-id> --secret <hex>
```

Each run prints its secret and endpoint id; without `--secret` a new key is generated.
