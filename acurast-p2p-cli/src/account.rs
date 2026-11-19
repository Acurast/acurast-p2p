//! Account ids as given on the command line: SS58 addresses (any network prefix) or 32-byte hex.

use blake2::{Blake2b512, Digest};

/// Parses an SS58 address or a `0x`-prefixed 32-byte hex account id.
pub fn parse(input: &str) -> Result<[u8; 32], String> {
    if let Some(hex) = input.strip_prefix("0x") {
        return hex::decode(hex)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| format!("invalid account {input}: expected 32 bytes of hex"));
    }
    ss58_decode(input).ok_or_else(|| format!("invalid account {input}: not an SS58 address"))
}

fn ss58_decode(address: &str) -> Option<[u8; 32]> {
    let data = bs58::decode(address).into_vec().ok()?;
    let prefix_len = match data.first()? {
        0..=63 => 1,
        64..=127 => 2,
        _ => return None,
    };
    if data.len() != prefix_len + 32 + 2 {
        return None;
    }
    let (body, checksum) = data.split_at(prefix_len + 32);
    let hash = Blake2b512::new()
        .chain_update(b"SS58PRE")
        .chain_update(body)
        .finalize();
    if hash[..2] != *checksum {
        return None;
    }
    body[prefix_len..].try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Alice, with the generic Substrate prefix 42.
    const ALICE_SS58: &str = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY";
    const ALICE_HEX: &str = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";

    #[test]
    fn parses_ss58_and_hex_to_the_same_account() {
        assert_eq!(parse(ALICE_SS58).unwrap(), parse(ALICE_HEX).unwrap());
    }

    #[test]
    fn rejects_a_wrong_checksum() {
        let mut broken = ALICE_SS58.to_owned();
        broken.replace_range(10..11, if &broken[10..11] == "a" { "b" } else { "a" });
        assert!(parse(&broken).is_err());
    }

    #[test]
    fn rejects_short_hex() {
        assert!(parse("0x1234").is_err());
    }
}
