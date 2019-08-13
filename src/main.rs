#![warn(clippy::all)]

use std::io::{self, BufRead};
macro_rules! statusln {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)] {
            eprint!("[STATUS] ");
            eprintln!($($arg)*);
        }
    };
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdin_handle = stdin.lock();

    let mut buffer = String::new();

    loop {
        match stdin_handle.read_line(&mut buffer) {
            Ok(0) => {
                statusln!("Closing...");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: {}, closing...", e);
                break;
            }
        }
    }

    Ok(())
}
