use anyhow::Result;
use serde_json::{json, Value};
use tracing::info;

pub struct AuthState {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
}

impl AuthState {
    pub fn new() -> Self {
        AuthState {
            access_token: None,
            refresh_token: None,
            expires_at: None,
        }
    }

    /// Build authentication request using client_credentials
    pub fn build_auth_request(client_id: &str, client_secret: &str, request_id: u64) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
        })
    }

    /// Build refresh token request
    pub fn build_refresh_request(refresh_token: &str, request_id: u64) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "public/auth",
            "params": {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
        })
    }

    /// Process auth response and update state
    pub fn process_auth_response(&mut self, result: &Value) -> Result<()> {
        let access_token = result["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing access_token"))?;
        let refresh_token = result["refresh_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing refresh_token"))?;
        let expires_in = result["expires_in"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("Missing expires_in"))?;

        let now = chrono::Utc::now().timestamp();
        self.access_token = Some(access_token.to_string());
        self.refresh_token = Some(refresh_token.to_string());
        self.expires_at = Some(now + expires_in);

        info!("Authenticated successfully, expires in {}s", expires_in);
        Ok(())
    }

    /// Check if token needs refresh (within 60s of expiry)
    pub fn needs_refresh(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => {
                let now = chrono::Utc::now().timestamp();
                now >= expires_at - 60
            }
            None => true,
        }
    }

    /// Get the refresh token for re-auth, if available
    pub fn get_refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }

    pub fn is_authenticated(&self) -> bool {
        self.access_token.is_some() && !self.needs_refresh()
    }
}
