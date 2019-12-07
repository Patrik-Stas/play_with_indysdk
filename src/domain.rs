use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub wallet_storage: WalletStorageConfig
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletStorageConfig {
    pub wallet_config: WalletConfig,
    pub wallet_credentials: WalletCredentials,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletConfig {
    pub id: String,
    pub storage_type: String,
    pub storage_config: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletCredentials {
    pub key: String,
    pub storage_credentials: Value,
}
