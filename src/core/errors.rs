#[derive(Debug)]
pub enum RelationErrors {
    /// The table with the given name was not found.
    RelationNotFound,
    /// Attempted to save a table with a name that already exists.
    RelationAlreadyExists,
    /// The select column was not found in the table.
    ColumnNotFound(String),
    /// Unspecified write error when saving the table.
    WriteError(String),
    /// Unspecified read error when loading the table.
    ReadError(String),
    /// Generic unspecified error.
    Error(String),

    InvalidInput(String),
}

impl From<csv::Error> for RelationErrors {
    fn from(_: csv::Error) -> Self {
        RelationErrors::ReadError("Error reading csv".to_string())
    }
}

impl From<std::io::Error> for RelationErrors {
    fn from(_: std::io::Error) -> Self {
        RelationErrors::ReadError("Error reading file".to_string())
    }
}
