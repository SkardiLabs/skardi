//! Synthetic mock source pack: proves the source-pack abstraction without
//! a real SaaS. The mock gateway in tests implements the `mock.list_items`
//! action with page-number pagination.

use std::sync::OnceLock;

use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<SourcePack> = OnceLock::new();

/// The synthetic mock pack, parsed once from the embedded YAML asset.
pub fn pack() -> &'static SourcePack {
    loader::builtin("mock.yaml", include_str!("mock.yaml"), &PACK)
}
