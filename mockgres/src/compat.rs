pub const POSTGRES_COMPAT_VERSION: &str = "18.4";
pub const POSTGRES_COMPAT_VERSION_NUM: &str = "180004";

pub(crate) fn server_version_string() -> String {
    format!("PostgreSQL {POSTGRES_COMPAT_VERSION} on mockgres")
}
