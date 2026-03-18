use std::cell::Cell;
use std::fs::File;
use std::io;
use std::io::Write;

thread_local! {
    static CURRENT_OP_INDEX: Cell<Option<u64>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Abort,
    IoErr,
    Partial(usize),
    TornAbort(usize),
    CorruptAbort(usize),
    DiskFull(usize),
}

pub struct OpIndexGuard {
    prev: Option<u64>,
}

impl Drop for OpIndexGuard {
    fn drop(&mut self) {
        let prev = self.prev;
        CURRENT_OP_INDEX.with(|c| c.set(prev));
    }
}

pub fn set_current_op_index(op_index: u64) -> OpIndexGuard {
    let prev = CURRENT_OP_INDEX.with(|c| {
        let prev = c.get();
        c.set(Some(op_index));
        prev
    });
    OpIndexGuard { prev }
}

pub fn hit(name: &'static str) {
    if action_for(name) == Some(Action::Abort) {
        std::process::abort();
    }
}

pub fn io_err(name: &'static str) -> io::Result<()> {
    if matches!(action_for(name), Some(Action::IoErr)) {
        return Err(io::Error::other("injected io error"));
    }
    Ok(())
}

pub fn write_all(name: &'static str, file: &mut File, buf: &[u8]) -> io::Result<()> {
    match action_for(name) {
        None => file.write_all(buf),
        Some(Action::Abort) => std::process::abort(),
        Some(Action::IoErr) => Err(io::Error::other("injected io error")),
        Some(Action::Partial(n)) => {
            let n = n.min(buf.len());
            if n > 0 {
                file.write_all(&buf[..n])?;
            }
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "injected partial write",
            ))
        }
        Some(Action::DiskFull(n)) => {
            let n = n.min(buf.len());
            if n > 0 {
                file.write_all(&buf[..n])?;
            }
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "injected disk full",
            ))
        }
        Some(Action::TornAbort(n)) => {
            let n = n.min(buf.len());
            if n > 0 {
                let _ = file.write_all(&buf[..n]);
            }
            std::process::abort();
        }
        Some(Action::CorruptAbort(n)) => {
            let mut tmp = buf.to_vec();
            if !tmp.is_empty() {
                let idx = n.min(tmp.len() - 1);
                tmp[idx] ^= 0xFF;
            }
            let _ = file.write_all(&tmp);
            std::process::abort();
        }
    }
}

pub fn sync_data(name: &'static str, file: &File) -> io::Result<()> {
    match action_for(name) {
        None | Some(Action::Abort) | Some(Action::TornAbort(_)) | Some(Action::CorruptAbort(_)) => {
            file.sync_data()
        }
        Some(Action::IoErr) => Err(io::Error::other("injected io error")),
        Some(Action::Partial(_)) => Err(io::Error::other("injected io error")),
        Some(Action::DiskFull(_)) => Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "injected disk full",
        )),
    }
}

fn action_for(name: &'static str) -> Option<Action> {
    let enabled = std::env::var("GRANITEDB_FAILPOINT").ok()?;
    if enabled != name {
        return None;
    }

    if let Ok(target_op) = std::env::var("GRANITEDB_FAILPOINT_OP")
        && let Ok(target_op) = target_op.parse::<u64>()
        && !matches_op(target_op)
    {
        return None;
    }

    let action = std::env::var("GRANITEDB_FAILPOINT_ACTION").unwrap_or_else(|_| "abort".into());
    Some(parse_action(&action))
}

fn parse_action(action: &str) -> Action {
    if action == "ioerr" {
        return Action::IoErr;
    }
    if let Some(v) = action.strip_prefix("partial:") {
        if let Ok(n) = v.parse::<usize>() {
            return Action::Partial(n);
        }
        return Action::Partial(0);
    }
    if let Some(v) = action.strip_prefix("diskfull:") {
        if let Ok(n) = v.parse::<usize>() {
            return Action::DiskFull(n);
        }
        return Action::DiskFull(0);
    }
    if let Some(v) = action.strip_prefix("torn_abort:") {
        if let Ok(n) = v.parse::<usize>() {
            return Action::TornAbort(n);
        }
        return Action::TornAbort(0);
    }
    if let Some(v) = action.strip_prefix("corrupt_abort:") {
        if let Ok(n) = v.parse::<usize>() {
            return Action::CorruptAbort(n);
        }
        return Action::CorruptAbort(0);
    }
    Action::Abort
}

fn matches_op(target: u64) -> bool {
    let cur = CURRENT_OP_INDEX.with(|c| c.get());
    cur == Some(target)
}
