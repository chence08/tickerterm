//! Pinned symbols, kept across runs in a plain text file (one symbol per line) that is also
//! fine to edit by hand: `$XDG_CONFIG_HOME/tickerterm/favorites.txt`, else `~/.config/...`.

use std::collections::HashSet;
use std::path::PathBuf;

fn path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("tickerterm").join("favorites.txt"))
}

pub fn load() -> HashSet<String> {
    let text = path().and_then(|p| std::fs::read_to_string(p).ok()).unwrap_or_default();
    text.lines().map(|l| l.trim().to_uppercase()).filter(|l| !l.is_empty() && !l.starts_with('#')).collect()
}

/// Returns an error message if the list could not be saved.
pub fn save(pins: &HashSet<String>) -> Result<(), String> {
    let path = path().ok_or("no home directory to save favourites in")?;
    let mut names: Vec<&str> = pins.iter().map(String::as_str).collect();
    names.sort_unstable();
    let mut body = names.join("\n");
    body.push('\n');
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    // write-then-rename so a crash never leaves a half-written list
    let tmp = path.with_extension("txt.tmp");
    std::fs::write(&tmp, body).and_then(|_| std::fs::rename(&tmp, &path)).map_err(|e| e.to_string())
}
