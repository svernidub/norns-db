use crate::column::Column;

/// A row is a sequence of column values.
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct Row(pub Vec<Column>);
