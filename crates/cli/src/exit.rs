//! Exit codes a command chooses for itself.
//!
//! Most commands keep the CLI's long-standing contract — `1` for any failure,
//! `2` for a server that cannot be reached — and `main` derives it from the
//! error. The agent-onboarding commands publish a richer table (design
//! 2026-10-27 §2.4.2) that a skill branches on, and `login --control` has one
//! code of its own, so those return an [`Exit`] carrying the code instead.

use std::fmt;

/// Exit with `code`.
///
/// `message` is what `main` prints as `error: …`. `None` means the command
/// already reported the failure on its own channel — the `--json` envelope on
/// stdout, which must stay the only thing a `--json` caller has to parse — so
/// `main` prints nothing more.
#[derive(Debug)]
pub struct Exit {
    pub code: u8,
    pub message: Option<String>,
}

impl Exit {
    /// Already reported; only the code is left to deliver.
    pub fn reported(code: u8) -> Exit {
        Exit {
            code,
            message: None,
        }
    }
}

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(message) => f.write_str(message),
            None => write!(f, "exit {}", self.code),
        }
    }
}

impl std::error::Error for Exit {}

#[cfg(test)]
mod tests {
    use super::Exit;

    /// What an `Exit` reads as when something prints it as an error — a
    /// context chain, a test's `{err}` — rather than through `main`.
    #[test]
    fn an_exit_reads_as_its_message_or_its_code() {
        let with_message = Exit {
            code: 6,
            message: Some("approved without control".to_string()),
        };
        assert_eq!(with_message.to_string(), "approved without control");
        assert_eq!(Exit::reported(4).to_string(), "exit 4");
        assert_eq!(Exit::reported(4).message, None);
        let err: anyhow::Error = Exit::reported(5).into();
        assert_eq!(err.downcast_ref::<Exit>().map(|e| e.code), Some(5));
    }
}
