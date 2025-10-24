pub mod error;
pub mod models;
pub mod interface;

cfg_if::cfg_if! {
    if #[cfg(feature = "sqlite")]
    {
        pub mod sqlite;
        pub mod sqlite_metastore;
        pub use sqlite_metastore::*;
    } else {
        pub mod metastore;
        pub use metastore::*;
    }
}

pub use error::Error;
pub use models::*;
pub use interface::*;
