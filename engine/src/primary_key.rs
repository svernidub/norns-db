#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, bincode::Encode, bincode::Decode)]
pub enum PrimaryKey {
    Integer(i32),
    BigInteger(i64),
    Varchar(String),
    Timestamp(i64),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PrimaryKeyType {
    Integer,
    BigInteger,
    Varchar,
    Timestamp,
}

impl PrimaryKey {
    #[allow(clippy::match_like_matches_macro)]
    pub fn is_a(&self, key_type: PrimaryKeyType) -> bool {
        match (self, key_type) {
            (PrimaryKey::Integer(_), PrimaryKeyType::Integer) => true,
            (PrimaryKey::BigInteger(_), PrimaryKeyType::BigInteger) => true,
            (PrimaryKey::Varchar(_), PrimaryKeyType::Varchar) => true,
            (PrimaryKey::Timestamp(_), PrimaryKeyType::Timestamp) => true,
            _ => false,
        }
    }

    pub fn pk_type(&self) -> PrimaryKeyType {
        use PrimaryKeyType::*;

        match self {
            Self::Integer(_) => Integer,
            Self::BigInteger(_) => BigInteger,
            Self::Varchar(_) => Varchar,
            Self::Timestamp(_) => Timestamp,
        }
    }
}
