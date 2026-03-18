use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use dbcore::error::NornsDbError;

pub enum ApiError {
    Db(NornsDbError),
    BadRequest(String),
    NotFound,
}

impl From<NornsDbError> for ApiError {
    fn from(err: NornsDbError) -> Self {
        ApiError::Db(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            ApiError::Db(ref err) => match err {
                NornsDbError::TableNotFound { .. } => (StatusCode::NOT_FOUND, err.to_string()),
                NornsDbError::TableAlreadyExists { .. } => (StatusCode::CONFLICT, err.to_string()),
                NornsDbError::PrimaryKeyTypeMismatch { .. }
                | NornsDbError::ColumnCountMismatch { .. }
                | NornsDbError::ColumnTypeMismatch { .. } => {
                    (StatusCode::BAD_REQUEST, err.to_string())
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            },
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
