/// Read with timeout options from the environment variables
#[macro_export]
macro_rules! read_exact_timeout {
    ($self:expr, $dst:expr, $read_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($read_timeout, $self.read_exact($dst)).await {
                Ok(res) => match res {
                    Ok(size) => Ok(size),
                    Err(read_err) => Err(read_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}

/// Write with timeout options from the environment variables
#[macro_export]
macro_rules! write_all_timeout {
    ($self:expr, $src:expr, $write_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($write_timeout, $self.write_all($src)).await {
                Ok(res) => match res {
                    Ok(size) => Ok(size),
                    Err(write_err) => Err(write_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}

/// Connect a stream with timeout options from the environment variables
#[macro_export]
macro_rules! connect_timeout {
    ($addr:expr, $connect_timeout:expr) => {
        async move {
            use tokio::net::TcpStream;
            use tokio::time::timeout;

            match timeout($connect_timeout, TcpStream::connect($addr)).await {
                Ok(res) => match res {
                    Ok(stream) => Ok(stream),
                    Err(connect_err) => Err(connect_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}
