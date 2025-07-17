#[derive(Debug)]
pub struct SurrealSocketError {
    pub message: String,
}

impl SurrealSocketError {
    pub fn new(message: &str) -> Self {
        SurrealSocketError {
            message: message.to_string(),
        }
    }
}

impl From<surrealdb::Error> for SurrealSocketError {
    fn from(e: surrealdb::Error) -> Self {
        SurrealSocketError::new(&format!("SurrealDB Operation error: {e}"))
    }
}

impl From<serde_json::error::Error> for SurrealSocketError {
    fn from(e: serde_json::error::Error) -> Self {
        SurrealSocketError::new(&format!("serde_json error: {e:?}"))
    }
}

impl From<String> for SurrealSocketError {
    fn from(e: String) -> Self {
        SurrealSocketError::new(&format!("SurrealDB error: {e}"))
    }
}

impl From<&str> for SurrealSocketError {
    fn from(e: &str) -> Self {
        SurrealSocketError::new(&format!("SurrealDB error: {e}"))
    }
}
