//! Best bid and ask of USDC/USDT on Binance spot, from the `usdcusdt@bookTicker` stream.
//!
//! Spot lives on a different host from the futures streams, so this is a second connection. It runs
//! on its own thread with a blocking socket; each update is published into a shared slot and the
//! main loop is woken through a pipe it polls alongside the futures socket and the keyboard, so the
//! line is redrawn as soon as the quote changes without the board waiting on this connection.

use serde::Deserialize;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tungstenite::Message;

const HOST: &str = "stream.binance.com";
const PORT: u16 = 9443;
const URL: &str = "wss://stream.binance.com:9443/ws/usdcusdt@bookTicker";

/// The stream's payload: `{"u":400900217,"s":"USDCUSDT","b":"1.00010000","B":"31.21","a":"1.00020000","A":"40.66"}`
#[derive(Deserialize)]
struct Wire<'a> {
    u: u64,
    #[serde(borrow)]
    b: &'a str,
    #[serde(rename = "B", borrow)]
    bq: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(rename = "A", borrow)]
    aq: &'a str,
}

#[derive(Clone, Default)]
pub struct Book {
    /// order book update id
    pub u: u64,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    pub live: bool,
    pub at: Option<Instant>,
    pub updates: u64,
}

pub struct Feed {
    pub book: Arc<Mutex<Book>>,
    /// set by the feed when it has published something the main loop has not picked up yet
    pub pending: Arc<AtomicBool>,
    /// read end of the wake-up pipe, for poll()
    pub wake_fd: i32,
}

impl Feed {
    /// Take the latest quote if there is a new one, emptying the wake-up pipe.
    pub fn take(&self) -> Option<Book> {
        if !self.pending.swap(false, Ordering::AcqRel) {
            return None;
        }
        let mut buf = [0u8; 64];
        while unsafe { libc::read(self.wake_fd, buf.as_mut_ptr() as *mut _, buf.len()) } > 0 {}
        Some(self.book.lock().unwrap().clone())
    }
}

fn session(book: &Mutex<Book>, publish: &dyn Fn()) -> Result<(), String> {
    let addr = (HOST, PORT).to_socket_addrs().map_err(|e| format!("dns: {e}"))?.next().ok_or("dns: no addresses")?;
    let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).map_err(|e| e.to_string())?;
    tcp.set_nodelay(true).map_err(|e| e.to_string())?;
    // the server pings every 20s, so a minute of silence means the connection is dead
    let _ = tcp.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(10)));
    let (mut ws, _) = tungstenite::client_tls(URL, tcp).map_err(|e| format!("handshake: {e}"))?;
    loop {
        match ws.read().map_err(|e| e.to_string())? {
            Message::Text(txt) => {
                let Ok(w) = serde_json::from_str::<Wire>(&txt) else { continue };
                let f = |s: &str| s.parse::<f64>().unwrap_or(0.0);
                let mut b = book.lock().unwrap();
                (b.u, b.bid, b.bid_qty, b.ask, b.ask_qty) = (w.u, f(w.b), f(w.bq), f(w.a), f(w.aq));
                b.live = true;
                b.at = Some(Instant::now());
                b.updates += 1;
                drop(b);
                publish();
            }
            // tungstenite queues the pong; reading again flushes it
            Message::Ping(_) => ws.flush().map_err(|e| e.to_string())?,
            Message::Close(_) => return Err("closed by server".into()),
            _ => {}
        }
    }
}

pub fn spawn() -> Feed {
    let mut fds = [0i32; 2];
    unsafe {
        libc::pipe(fds.as_mut_ptr());
        for fd in fds {
            libc::fcntl(fd, libc::F_SETFL, libc::fcntl(fd, libc::F_GETFL) | libc::O_NONBLOCK);
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    let book = Arc::new(Mutex::new(Book::default()));
    let pending = Arc::new(AtomicBool::new(false));
    let feed = Feed { book: book.clone(), pending: pending.clone(), wake_fd: fds[0] };
    let wfd = fds[1];
    std::thread::spawn(move || {
        // one byte in the pipe per unread publication: a burst of quotes wakes the main loop once
        let publish = || {
            if !pending.swap(true, Ordering::AcqRel) {
                unsafe { libc::write(wfd, b"x".as_ptr() as *const _, 1) };
            }
        };
        let mut backoff = 1;
        loop {
            let started = Instant::now();
            let _ = session(&book, &publish);
            book.lock().unwrap().live = false;
            publish();
            if started.elapsed() > Duration::from_secs(30) {
                backoff = 1;
            }
            std::thread::sleep(Duration::from_secs(backoff));
            backoff = (backoff * 2).min(30);
        }
    });
    feed
}

/// One pinned line: both sides of the book, the spread, where the mid sits against 1.0000, and how
/// the resting size at the touch is split.
pub fn line(s: &mut Vec<u8>, b: &Book, hum: fn(&mut Vec<u8>, f64, usize)) {
    s.extend_from_slice(b"\x1b[0;1m USDC/USDT\x1b[0;2m spot\x1b[0m");
    if b.updates == 0 {
        s.extend_from_slice(b"\x1b[2m | connecting\x1b[0m");
        return;
    }
    let dim = if b.live { "" } else { "2;" };
    // a pip on this pair is 0.0001; print one more digit in case the tick ever gets finer
    let px = |v: f64| {
        let t = format!("{v:.5}");
        t.strip_suffix('0').map(str::to_string).unwrap_or(t)
    };
    let _ = write!(s, " | \x1b[0;{dim}32mbid {} x ", px(b.bid));
    hum(s, b.bid_qty, 0);
    let _ = write!(s, "\x1b[0m | \x1b[0;{dim}31mask {} x ", px(b.ask));
    hum(s, b.ask_qty, 0);
    let mid = (b.bid + b.ask) / 2.0;
    let _ = write!(s, "\x1b[0;{dim}m | spread {:.2}bp | mid {:.6} ({:+.2}bp vs 1)", (b.ask - b.bid) / mid * 1e4, mid, (mid - 1.0) * 1e4);
    let tot = b.bid_qty + b.ask_qty;
    if tot > 0.0 {
        let share = b.bid_qty / tot;
        let n = (share * 10.0).round() as usize;
        let _ = write!(s, " | touch {:>3.0}% bid \x1b[0;{dim}32m{}\x1b[0;{dim}31m{}\x1b[0;{dim}m", share * 100.0, "█".repeat(n), "█".repeat(10 - n));
    }
    let _ = write!(s, " | u {}", b.u);
    if !b.live {
        s.extend_from_slice(b" | \x1b[0;33mreconnecting");
    }
    s.extend_from_slice(b"\x1b[0m");
}
