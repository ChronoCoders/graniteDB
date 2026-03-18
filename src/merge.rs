use std::sync::Arc;

use crate::error::Result;

pub trait MergeOperator: Send + Sync {
    fn full_merge(
        &self,
        key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Option<Vec<u8>>>;
}

#[derive(Clone, Default)]
pub struct AppendMergeOperator;

impl MergeOperator for AppendMergeOperator {
    fn full_merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Option<Vec<u8>>> {
        if existing_value.is_none() && operands.is_empty() {
            return Ok(None);
        }
        let mut out = Vec::new();
        if let Some(v) = existing_value {
            out.extend_from_slice(v);
        }
        for op in operands {
            out.extend_from_slice(op);
        }
        Ok(Some(out))
    }
}

pub fn default_merge_operator() -> Arc<dyn MergeOperator> {
    Arc::new(AppendMergeOperator)
}
