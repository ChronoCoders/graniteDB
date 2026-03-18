use std::cell::Cell;

thread_local! {
    static CURRENT_OP_INDEX: Cell<Option<u64>> = const { Cell::new(None) };
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
    let enabled = std::env::var("GRANITEDB_FAILPOINT").ok();
    let enabled = match enabled {
        Some(v) => v,
        None => return,
    };
    if enabled != name {
        return;
    }

    if let Ok(target_op) = std::env::var("GRANITEDB_FAILPOINT_OP")
        && let Ok(target_op) = target_op.parse::<u64>()
    {
        let cur = CURRENT_OP_INDEX.with(|c| c.get());
        if cur != Some(target_op) {
            return;
        }
    }

    let action = std::env::var("GRANITEDB_FAILPOINT_ACTION").unwrap_or_else(|_| "abort".into());
    match action.as_str() {
        "abort" => std::process::abort(),
        _ => std::process::abort(),
    }
}
