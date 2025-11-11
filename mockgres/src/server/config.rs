#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub database_name: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            database_name: "postgres".to_string(),
        }
    }
}
