/// Represents a value stored in SS Table in the LSM tree.
///
/// Since LSM Tree supposes several sequential tables where keys can be repeated,
/// Tombstone represents a deleted value that terminates a search.
#[derive(bincode::Encode, bincode::Decode, Clone)]
pub enum Value<T>
where
    T: Clone,
{
    /// Data is present.
    Data(T),

    /// Data is deleted, further search is not required.
    Tombstone,
}
