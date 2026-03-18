use std::cell::Cell;
use std::io;

thread_local! {
    static CURRENT_OP_INDEX: Cell<Option<u64>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Abort,
    IoErr,
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
    if action_for(name) == Some(Action::IoErr) {
        return Err(io::Error::other("injected io error"));
    }
    Ok(())
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
    Some(match action.as_str() {
        "ioerr" => Action::IoErr,
        _ => Action::Abort,
    })
}

fn matches_op(target: u64) -> bool {
    let cur = CURRENT_OP_INDEX.with(|c| c.get());
    cur == Some(target)
}
