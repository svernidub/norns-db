/// Represents a table/row cell that holds a typed value.
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Column {
    Boolean(bool),

    Integer(i32),
    BigInteger(i64),

    Float(f32),
    Double(f64),

    // TODO: rework later as blob.
    Varchar(String),

    Timestamp(i64),
}

#[derive(Debug, Copy, Clone, bincode::Encode, bincode::Decode)]
pub enum ColumnType {
    Boolean,

    Integer,
    BigInteger,
    Float,
    Double,

    Varchar,

    Timestamp,
}

impl Column {
    #[allow(clippy::match_like_matches_macro)]
    pub fn is_a(&self, column_type: ColumnType) -> bool {
        match (self, column_type) {
            (Self::Boolean(_), ColumnType::Boolean) => true,
            (Self::Integer(_), ColumnType::Integer) => true,
            (Self::BigInteger(_), ColumnType::BigInteger) => true,
            (Self::Float(_), ColumnType::Float) => true,
            (Self::Double(_), ColumnType::Double) => true,
            (Self::Varchar(_), ColumnType::Varchar) => true,
            (Self::Timestamp(_), ColumnType::Timestamp) => true,
            _ => false,
        }
    }

    pub fn col_type(&self) -> ColumnType {
        use ColumnType::*;

        match self {
            Self::Boolean(_) => Boolean,
            Self::Integer(_) => Integer,
            Self::BigInteger(_) => BigInteger,
            Self::Float(_) => Float,
            Self::Double(_) => Double,
            Self::Varchar(_) => Varchar,
            Self::Timestamp(_) => Timestamp,
        }
    }
}
