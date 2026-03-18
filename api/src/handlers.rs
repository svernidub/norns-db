use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use dbcore::error::NornsDbError;
use engine::Database;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::ApiError,
    models::{self, CreateTableRequest},
};

pub async fn create_table(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
    Json(request): Json<CreateTableRequest>,
) -> Result<StatusCode, ApiError> {
    let schema = request.into();

    db.create_table(table_name, schema).await?;
    Ok(StatusCode::CREATED)
}

pub async fn drop_table(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
) -> Result<StatusCode, ApiError> {
    db.drop_table(table_name).await?;
    Ok(StatusCode::OK)
}

pub async fn list_rows(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
) -> Result<Json<Vec<HashMap<String, models::ApiValue>>>, ApiError> {
    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let items = db.list_rows(&table_name).await?;

    let json = items
        .into_iter()
        .map(|(pk, row)| models::row_with_pk_to_json(pk, row, &schema))
        .collect();

    Ok(Json(json))
}

pub async fn insert(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
    Json(body): Json<HashMap<String, serde_json::Value>>,
) -> Result<StatusCode, ApiError> {
    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let primary_key =
        models::parse_primary_key(&key, schema.primary_key_type).map_err(ApiError::BadRequest)?;

    let row = models::json_to_row(body, &schema.columns).map_err(ApiError::BadRequest)?;

    db.insert(&table_name, primary_key, row).await?;
    Ok(StatusCode::CREATED)
}

pub async fn get_row(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
) -> Result<Json<HashMap<String, models::ApiValue>>, ApiError> {
    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let primary_key =
        models::parse_primary_key(&key, schema.primary_key_type).map_err(ApiError::BadRequest)?;

    let row = db
        .get(&table_name, &primary_key)
        .await?
        .ok_or(ApiError::NotFound)?;

    let json = models::row_to_json(row, &schema.columns);
    Ok(Json(json))
}

pub async fn delete_row(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let primary_key =
        models::parse_primary_key(&key, schema.primary_key_type).map_err(ApiError::BadRequest)?;

    db.delete(&table_name, primary_key)
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(StatusCode::OK)
}

fn table_not_found(name: &str) -> ApiError {
    ApiError::Db(NornsDbError::TableNotFound {
        name: name.to_string(),
    })
}
