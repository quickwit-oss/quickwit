use std::fmt;

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", &self.message)
    }
}

impl std::error::Error for ParseError {
    fn description(&self) -> &str {
        &self.message
    }
}
