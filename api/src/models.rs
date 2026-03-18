use engine::{
    TableSchema,
    column::{Column, ColumnType},
    primary_key::{PrimaryKey, PrimaryKeyType},
    row::Row,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApiPrimaryKeyType {
    Integer,
    BigInteger,
    Varchar,
    Timestamp,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApiColumnType {
    Boolean,
    Integer,
    BigInteger,
    Float,
    Double,
    Varchar,
    Timestamp,
}

#[derive(Deserialize)]
pub struct CreateTableRequest {
    primary_key: PrimaryKeyDefinition,
    columns: Vec<ColumnDefinition>,
}

#[derive(Deserialize)]
struct PrimaryKeyDefinition {
    name: String,

    #[serde(rename = "type")]
    key_type: ApiPrimaryKeyType,
}

#[derive(Deserialize)]
struct ColumnDefinition {
    name: String,

    #[serde(rename = "type")]
    column_type: ApiColumnType,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum ApiValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl From<ApiPrimaryKeyType> for PrimaryKeyType {
    fn from(t: ApiPrimaryKeyType) -> Self {
        use ApiPrimaryKeyType::*;

        match t {
            Integer => PrimaryKeyType::Integer,
            BigInteger => PrimaryKeyType::BigInteger,
            Varchar => PrimaryKeyType::Varchar,
            Timestamp => PrimaryKeyType::Timestamp,
        }
    }
}

impl From<ApiColumnType> for ColumnType {
    fn from(t: ApiColumnType) -> Self {
        use ApiColumnType::*;

        match t {
            Boolean => ColumnType::Boolean,
            Integer => ColumnType::Integer,
            BigInteger => ColumnType::BigInteger,
            Float => ColumnType::Float,
            Double => ColumnType::Double,
            Varchar => ColumnType::Varchar,
            Timestamp => ColumnType::Timestamp,
        }
    }
}

impl From<CreateTableRequest> for TableSchema {
    fn from(
        CreateTableRequest {
            primary_key,
            columns,
        }: CreateTableRequest,
    ) -> Self {
        let columns = columns
            .into_iter()
            .map(|c| (c.name, c.column_type.into()))
            .collect();

        Self {
            primary_key_name: primary_key.name,
            primary_key_type: primary_key.key_type.into(),
            columns,
        }
    }
}

impl From<Column> for ApiValue {
    fn from(column: Column) -> Self {
        use Column::*;

        match column {
            Boolean(v) => ApiValue::Bool(v),
            Integer(v) => ApiValue::Int(v as i64),
            BigInteger(v) => ApiValue::Int(v),
            Float(v) => ApiValue::Float(v as f64),
            Double(v) => ApiValue::Float(v),
            Varchar(v) => ApiValue::String(v),
            Timestamp(v) => ApiValue::Int(v),
        }
    }
}

pub fn parse_primary_key(raw: &str, pk_type: PrimaryKeyType) -> Result<PrimaryKey, String> {
    match pk_type {
        PrimaryKeyType::Integer => {
            let v: i32 = raw
                .parse()
                .map_err(|e| format!("invalid integer key: {e}"))?;

            Ok(PrimaryKey::Integer(v))
        }
        PrimaryKeyType::BigInteger => {
            let v: i64 = raw
                .parse()
                .map_err(|e| format!("invalid big_integer key: {e}"))?;
            Ok(PrimaryKey::BigInteger(v))
        }
        PrimaryKeyType::Varchar => Ok(PrimaryKey::Varchar(raw.to_string())),
        PrimaryKeyType::Timestamp => {
            let v: i64 = raw
                .parse()
                .map_err(|e| format!("invalid timestamp key: {e}"))?;
            Ok(PrimaryKey::Timestamp(v))
        }
    }
}

pub fn json_to_row(
    body: HashMap<String, serde_json::Value>,
    schema_columns: &[(String, ColumnType)],
) -> Result<Row, String> {
    let mut columns = Vec::with_capacity(schema_columns.len());

    for (name, col_type) in schema_columns {
        let value = body
            .get(name)
            .ok_or_else(|| format!("missing column: {name}"))?;

        let column = json_value_to_column(value, *col_type, name)?;
        columns.push(column);
    }

    Ok(Row(columns))
}

fn json_value_to_column(
    value: &serde_json::Value,
    col_type: ColumnType,
    name: &str,
) -> Result<Column, String> {
    match col_type {
        ColumnType::Boolean => {
            let v = value
                .as_bool()
                .ok_or_else(|| format!("column {name}: expected boolean"))?;
            Ok(Column::Boolean(v))
        }
        ColumnType::Integer => {
            let v = value
                .as_i64()
                .ok_or_else(|| format!("column {name}: expected integer"))?;
            let v = i32::try_from(v)
                .map_err(|_| format!("column {name}: value out of range for integer"))?;
            Ok(Column::Integer(v))
        }
        ColumnType::BigInteger => {
            let v = value
                .as_i64()
                .ok_or_else(|| format!("column {name}: expected big_integer"))?;
            Ok(Column::BigInteger(v))
        }
        ColumnType::Float => {
            let v = value
                .as_f64()
                .ok_or_else(|| format!("column {name}: expected float"))?;
            Ok(Column::Float(v as f32))
        }
        ColumnType::Double => {
            let v = value
                .as_f64()
                .ok_or_else(|| format!("column {name}: expected double"))?;
            Ok(Column::Double(v))
        }
        ColumnType::Varchar => {
            let v = value
                .as_str()
                .ok_or_else(|| format!("column {name}: expected varchar (string)"))?;
            Ok(Column::Varchar(v.to_string()))
        }
        ColumnType::Timestamp => {
            let v = value
                .as_i64()
                .ok_or_else(|| format!("column {name}: expected timestamp (integer)"))?;
            Ok(Column::Timestamp(v))
        }
    }
}

pub fn row_to_json(row: Row, schema_columns: &[(String, ColumnType)]) -> HashMap<String, ApiValue> {
    row.0
        .into_iter()
        .zip(schema_columns.iter())
        .map(|(col, (name, _))| (name.clone(), col.into()))
        .collect()
}

pub fn primary_key_to_api_value(primary_key: PrimaryKey) -> ApiValue {
    match primary_key {
        PrimaryKey::Integer(v) => ApiValue::Int(v as i64),
        PrimaryKey::BigInteger(v) => ApiValue::Int(v),
        PrimaryKey::Varchar(v) => ApiValue::String(v),
        PrimaryKey::Timestamp(v) => ApiValue::Int(v),
    }
}

pub fn row_with_pk_to_json(
    primary_key: PrimaryKey,
    row: Row,
    schema: &TableSchema,
) -> HashMap<String, ApiValue> {
    let mut json = HashMap::with_capacity(1 + schema.columns.len());

    json.insert(
        schema.primary_key_name.clone(),
        primary_key_to_api_value(primary_key),
    );

    json.extend(row_to_json(row, &schema.columns));
    json
}
