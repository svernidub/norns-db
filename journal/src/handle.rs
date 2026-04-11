/// A Journal Handle.
///
/// An opaque handle for journal operations. Used for journal notification that
/// journaled data is committed on a specified offset.
#[derive(Clone, Debug)]
pub struct JournalHandle {
    pub(crate) offset: u64,
}
