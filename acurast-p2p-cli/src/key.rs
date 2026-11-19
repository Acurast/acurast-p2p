//! The manager's P2P key, stored hex-encoded in a file only its owner can read.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use acurast_p2p::SecretKey;

/// `$XDG_CONFIG_HOME/acurast-p2p/manager.key`, or `~/.config/acurast-p2p/manager.key`.
pub fn default_path() -> Result<PathBuf, String> {
    let config = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => {
            let home = std::env::var_os("HOME")
                .ok_or("cannot find the home folder: give the key file with --key")?;
            PathBuf::from(home).join(".config")
        }
    };
    Ok(config.join("acurast-p2p").join("manager.key"))
}

/// Generates a new key and stores it at `path`. Refuses to replace an existing key unless
/// `force`: the key registered on chain would be lost.
pub fn create(path: &Path, force: bool) -> Result<SecretKey, String> {
    if path.exists() && !force {
        return Err(format!(
            "{} already exists: a key registered on chain would be lost (use --force to replace it)",
            path.display()
        ));
    }
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    }
    let key = SecretKey::generate();
    write_private(path, hex::encode(key.to_bytes()).as_bytes())
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    Ok(key)
}

/// Reads the key stored at `path`.
pub fn load(path: &Path) -> Result<SecretKey, String> {
    let content = fs::read_to_string(path).map_err(|e| {
        format!(
            "cannot read the key {}: {e} (create one with `acurast-p2p init`)",
            path.display()
        )
    })?;
    let bytes: [u8; 32] = hex::decode(content.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| format!("{} does not contain a 32-byte hex key", path.display()))?;
    Ok(SecretKey::from_bytes(&bytes))
}

#[cfg(unix)]
fn write_private(path: &Path, content: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    // Truncating keeps the permissions of an existing file, so set them again.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(content)
}

#[cfg(not(unix))]
fn write_private(path: &Path, content: &[u8]) -> std::io::Result<()> {
    fs::write(path, content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_load_returns_the_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("manager.key");
        let key = create(&path, false).unwrap();
        assert_eq!(load(&path).unwrap().to_bytes(), key.to_bytes());
    }

    #[test]
    fn create_refuses_to_replace_a_key_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manager.key");
        let first = create(&path, false).unwrap();
        assert!(create(&path, false).is_err());
        assert_eq!(load(&path).unwrap().to_bytes(), first.to_bytes());
        let second = create(&path, true).unwrap();
        assert_ne!(second.to_bytes(), first.to_bytes());
    }

    #[cfg(unix)]
    #[test]
    fn the_key_file_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manager.key");
        create(&path, false).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn load_rejects_a_malformed_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manager.key");
        fs::write(&path, "not a key").unwrap();
        assert!(load(&path).is_err());
    }
}
