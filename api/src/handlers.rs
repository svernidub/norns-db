use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use dbcore::error::NornsDbError;
use engine::Database;
use metrics::{gauge, histogram};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tracing::instrument;

use crate::{
    error::ApiError,
    models::{self, CreateTableRequest},
};

#[instrument(skip(db, request))]
pub async fn create_table(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
    Json(request): Json<CreateTableRequest>,
) -> Result<StatusCode, ApiError> {
    let schema = request.into();

    db.create_table(table_name, schema).await?;
    Ok(StatusCode::CREATED)
}

#[instrument(skip(db))]
pub async fn drop_table(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
) -> Result<StatusCode, ApiError> {
    db.drop_table(table_name).await?;
    Ok(StatusCode::OK)
}

#[instrument(skip(db))]
pub async fn list_rows(
    State(db): State<Arc<Database>>,
    Path(table_name): Path<String>,
) -> Result<Json<Vec<HashMap<String, models::ApiValue>>>, ApiError> {
    let start = Instant::now();

    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let items = db.list_rows(&table_name).await?;

    gauge!("norns_api_list_rows_count", "table_name" => table_name.clone()).set(items.len() as f64);

    let json = items
        .into_iter()
        .map(|(pk, row)| models::row_with_pk_to_json(pk, row, &schema))
        .collect();

    histogram!("norns_api_list_duration_ms", "table_name" => table_name.clone())
        .record(start.elapsed().as_millis() as f64);

    Ok(Json(json))
}

#[instrument(skip(db, body))]
pub async fn insert(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
    Json(body): Json<HashMap<String, serde_json::Value>>,
) -> Result<StatusCode, ApiError> {
    let start = Instant::now();

    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let primary_key =
        models::parse_primary_key(&key, schema.primary_key_type).map_err(ApiError::BadRequest)?;

    let row = models::json_to_row(body, &schema.columns).map_err(ApiError::BadRequest)?;

    db.insert(&table_name, primary_key, row).await?;

    histogram!("norns_api_insert_duration_us", "table_name" => table_name)
        .record(start.elapsed().as_micros() as f64);

    Ok(StatusCode::CREATED)
}

#[instrument(skip(db))]
pub async fn get_row(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
) -> Result<Json<HashMap<String, models::ApiValue>>, ApiError> {
    let start = Instant::now();

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

    histogram!("norns_api_get_duration_us", "table_name" => table_name)
        .record(start.elapsed().as_micros() as f64);

    Ok(Json(json))
}

#[instrument(skip(db))]
pub async fn delete_row(
    State(db): State<Arc<Database>>,
    Path((table_name, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let start = Instant::now();

    let schema = db
        .table_schema(&table_name)
        .await
        .ok_or_else(|| table_not_found(&table_name))?;

    let primary_key =
        models::parse_primary_key(&key, schema.primary_key_type).map_err(ApiError::BadRequest)?;

    db.delete(&table_name, primary_key).await?;

    histogram!("norns_api_delete_duration_us", "table_name" => table_name)
        .record(start.elapsed().as_micros() as f64);

    Ok(StatusCode::OK)
}

fn table_not_found(name: &str) -> ApiError {
    ApiError::Db(NornsDbError::TableNotFound {
        name: name.to_string(),
    })
}
