pub struct ClusterHandle {
    // Handle to control the daemon lifecycle
}

impl Default for ClusterHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterHandle {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn shutdown(self) {
        // trigger shutdown
    }
}
