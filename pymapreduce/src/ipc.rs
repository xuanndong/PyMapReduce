use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::process::Stdio;

#[allow(dead_code)]
pub struct IpcWorker {
    pub child: Child,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub pid: Option<u32>,
}

#[derive(Debug)]
pub enum IpcError {
    IoError(String),
    AppError(String),
}

impl IpcWorker {
    pub async fn spawn(env_id: Option<uuid::Uuid>) -> Result<Self, String> {
        let py_bin = std::env::var("PYTHON_EXECUTABLE").unwrap_or_else(|_| {
            pyo3::Python::with_gil(|py| {
                py.import("sys")
                    .and_then(|sys| sys.getattr("executable"))
                    .and_then(|exe| exe.extract::<String>())
                    .unwrap_or_else(|_| "python".to_string())
            })
        });

        let mut cmd = Command::new(py_bin);
        cmd.arg("-m").arg("pymapreduce.daemon")
           .stdin(Stdio::piped())
           .stdout(Stdio::piped())
           .stderr(Stdio::inherit());
           
        if let Some(id) = env_id {
            let shm_base = std::path::Path::new("/dev/shm");
            let env_dir = if shm_base.is_dir() {
                shm_base.join("pymapreduce").join("env").join(id.to_string())
            } else {
                std::env::temp_dir().join("pymapreduce").join("env").join(id.to_string())
            };
            if env_dir.exists() {
                cmd.env("PYTHONPATH", env_dir.to_str().unwrap());
            } else {
                let fallback = std::env::temp_dir().join("pymapreduce").join("env").join(id.to_string());
                if fallback.exists() {
                    cmd.env("PYTHONPATH", fallback.to_str().unwrap());
                }
            }
        }

        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                #[cfg(target_os = "linux")]
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                // Create a distinct process group for this worker and all its ML children
                libc::setpgid(0, 0);
                Ok(())
            });
        }

        let mut child = cmd.spawn().map_err(|e| e.to_string())?;
        let pid = child.id();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        
        Ok(Self { child, stdin, stdout, pid })
    }

    #[cfg(unix)]
    pub fn kill_group(&mut self, sig: libc::c_int) -> Result<(), std::io::Error> {
        if let Some(pid) = self.pid {
            let pgid = pid as i32;
            let res = unsafe { libc::kill(-pgid, sig) };
            if res != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ESRCH) {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub async fn kill_group(&mut self, _sig: i32) -> Result<(), std::io::Error> {
        self.child.kill().await
    }
    
    pub async fn send_and_receive(&mut self, payload: Vec<u8>) -> Result<Vec<u8>, IpcError> {
        let len = payload.len() as u32;
        self.stdin.write_all(&len.to_be_bytes()).await.map_err(|e| IpcError::IoError(e.to_string()))?;
        self.stdin.write_all(&payload).await.map_err(|e| IpcError::IoError(e.to_string()))?;
        self.stdin.flush().await.map_err(|e| IpcError::IoError(e.to_string()))?;
        
        let mut status = [0u8; 1];
        self.stdout.read_exact(&mut status).await.map_err(|e| IpcError::IoError(e.to_string()))?;
        
        let mut len_buf = [0u8; 4];
        self.stdout.read_exact(&mut len_buf).await.map_err(|e| IpcError::IoError(e.to_string()))?;
        
        let out_len = u32::from_be_bytes(len_buf) as usize;
        let mut out_buf = vec![0u8; out_len];
        self.stdout.read_exact(&mut out_buf).await.map_err(|e| IpcError::IoError(e.to_string()))?;
        
        if status[0] == 0 {
            Ok(out_buf)
        } else {
            Err(IpcError::AppError(String::from_utf8_lossy(&out_buf).to_string()))
        }
    }
}
