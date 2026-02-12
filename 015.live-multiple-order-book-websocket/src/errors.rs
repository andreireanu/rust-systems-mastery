use tokio::time::error::Elapsed;

#[allow(dead_code)]
pub enum AppError {
    Ws(tokio_tungstenite::tungstenite::Error),
    Timeout(Elapsed),
    Serde(serde_json::Error),
    FloatParse(std::num::ParseFloatError),
    IntParse(std::num::ParseIntError),
    KrakenWrongFormat,
    TcpListenerBindError(std::io::Error),
}

impl std::fmt::Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self) // Delegates to Display
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AppError::Ws(e) => write!(f, "WebSocket error: {}", e),
            AppError::Timeout(e) => write!(f, "Connection timeout: {}", e),
            AppError::Serde(e) => write!(f, "JSON parse error: {}", e),
            AppError::FloatParse(e) => write!(f, "Float parse error: {}", e),
            AppError::IntParse(e) => write!(f, "Int parse error: {}", e),
            AppError::KrakenWrongFormat => write!(f, "Kraken sent different format"),
            AppError::TcpListenerBindError(e) => write!(f, "TcpListener bind error: {}", e),
        }
    }
}

impl std::error::Error for AppError {}

impl From<tokio_tungstenite::tungstenite::Error> for AppError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        AppError::Ws(value)
    }
}

impl From<Elapsed> for AppError {
    fn from(value: Elapsed) -> Self {
        AppError::Timeout(value)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        AppError::Serde(value)
    }
}

impl From<std::num::ParseFloatError> for AppError {
    fn from(value: std::num::ParseFloatError) -> Self {
        AppError::FloatParse(value)
    }
}

impl From<std::num::ParseIntError> for AppError {
    fn from(value: std::num::ParseIntError) -> Self {
        AppError::IntParse(value)
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        AppError::TcpListenerBindError(value)
    }
}
