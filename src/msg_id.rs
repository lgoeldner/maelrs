use std::sync::atomic::{AtomicU32, Ordering};

static LAST_ID: AtomicU32 = AtomicU32::new(0);

pub fn create_unique() -> u32 {
    LAST_ID.fetch_add(1, Ordering::AcqRel)
}
