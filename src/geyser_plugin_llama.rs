/// Main entry for the PostgreSQL plugin
///
extern crate libloading;
use {
    crate::{
        accounts_selector::AccountsSelector,
        transaction_selector::TransactionSelector,
    },
    bs58,
    log::*,
    serde_derive::{Deserialize, Serialize},
    serde_json,
    solana_sdk::hash::{hash, Hash},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
    solana_measure::measure::Measure,
    solana_metrics::*,
    crossbeam_channel::{RecvTimeoutError, unbounded, Sender},
    std::{collections::HashMap, thread::{JoinHandle, Builder}, time::Duration, ffi::{CString, c_char, c_int}, fs, fs::File, io::Read},
    thiserror::Error,
};

enum JobStatus {
    Started,
    Completed,
}

#[derive(Default)]
struct Job {
}

#[derive(Default)]
pub struct GeyserPluginPostgres {
    accounts_selector: Option<AccountsSelector>,
    transaction_selector: Option<TransactionSelector>,
    batch_starting_slot: Option<u64>,
    worker_thread: Option<JoinHandle<()>>,
    jobs: HashMap<Hash, JobStatus>,
    job_sender: Option<Sender<llm::MLJob>>,
}

impl std::fmt::Debug for GeyserPluginPostgres {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/*#[link(name = "libllama")]
extern {
    fn run_llm(argc: c_int, argv: *const *const c_char) -> c_int;
}*/

/// The Configuration for the PostgreSQL plugin
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GeyserPluginPostgresConfig {
    /// The host name or IP of the PostgreSQL server
    pub host: Option<String>,

    /// The user name of the PostgreSQL server.
    pub user: Option<String>,

    /// The port number of the PostgreSQL database, the default is 5432
    pub port: Option<u16>,

    /// The connection string of PostgreSQL database, if this is set
    /// `host`, `user` and `port` will be ignored.
    pub connection_str: Option<String>,

    /// Controls the number of threads establishing connections to
    /// the PostgreSQL server. The default is 10.
    pub threads: Option<usize>,

    /// Controls the batch size when bulk loading accounts.
    /// The default is 10.
    pub batch_size: Option<usize>,

    /// Controls whether to panic the validator in case of errors
    /// writing to PostgreSQL server. The default is false
    pub panic_on_db_errors: Option<bool>,

    /// Indicates whether to store historical data for accounts
    pub store_account_historical_data: Option<bool>,

    /// Controls whether to use SSL based connection to the database server.
    /// The default is false
    pub use_ssl: Option<bool>,

    /// Specify the path to PostgreSQL server's certificate file
    pub server_ca: Option<String>,

    /// Specify the path to the local client's certificate file
    pub client_cert: Option<String>,

    /// Specify the path to the local client's private PEM key file.
    pub client_key: Option<String>,

    /// Controls whether to index the token owners. The default is false
    pub index_token_owner: Option<bool>,

    /// Controls whetherf to index the token mints. The default is false
    pub index_token_mint: Option<bool>,

    /// Controls if this plugin can read the database on_load() to find heighest slot
    /// and ignore upsetr accounts (at_startup) that should already exist in DB
    #[serde(default)]
    pub skip_upsert_existing_accounts_at_startup: bool,
}

#[derive(Error, Debug)]
pub enum GeyserPluginPostgresError {
    #[error("Error connecting to the backend data store. Error message: ({msg})")]
    DataStoreConnectionError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    DataSchemaError { msg: String },

    #[error("Error preparing data store schema. Error message: ({msg})")]
    ConfigurationError { msg: String },

    #[error("Replica account V0.0.1 not supported anymore")]
    ReplicaAccountV001NotSupported,

    #[error("LLM argument error")]
    LlmArgumentError,
}

type RunLlmFunc = unsafe fn(argc: c_int, argv: *const *const c_char) -> c_int;

impl GeyserPlugin for GeyserPluginPostgres {
    fn name(&self) -> &'static str {
        "GeyserPluginPostgres"
    }

    /// Do initialization for the PostgreSQL plugin.
    ///
    /// # Format of the config file:
    /// * The `accounts_selector` section allows the user to controls accounts selections.
    /// "accounts_selector" : {
    ///     "accounts" : \["pubkey-1", "pubkey-2", ..., "pubkey-n"\],
    /// }
    /// or:
    /// "accounts_selector" = {
    ///     "owners" : \["pubkey-1", "pubkey-2", ..., "pubkey-m"\]
    /// }
    /// Accounts either satisyfing the accounts condition or owners condition will be selected.
    /// When only owners is specified,
    /// all accounts belonging to the owners will be streamed.
    /// The accounts field supports wildcard to select all accounts:
    /// "accounts_selector" : {
    ///     "accounts" : \["*"\],
    /// }
    /// * "host", optional, specifies the PostgreSQL server.
    /// * "user", optional, specifies the PostgreSQL user.
    /// * "port", optional, specifies the PostgreSQL server's port.
    /// * "connection_str", optional, the custom PostgreSQL connection string.
    /// Please refer to https://docs.rs/postgres/0.19.2/postgres/config/struct.Config.html for the connection configuration.
    /// When `connection_str` is set, the values in "host", "user" and "port" are ignored. If `connection_str` is not given,
    /// `host` and `user` must be given.
    /// "store_account_historical_data", optional, set it to 'true', to store historical account data to account_audit
    /// table.
    /// * "threads" optional, specifies the number of worker threads for the plugin. A thread
    /// maintains a PostgreSQL connection to the server. The default is '10'.
    /// * "batch_size" optional, specifies the batch size of bulk insert when the AccountsDb is created
    /// from restoring a snapshot. The default is '10'.
    /// * "panic_on_db_errors", optional, contols if to panic when there are errors replicating data to the
    /// PostgreSQL database. The default is 'false'.
    /// * "transaction_selector", optional, controls if and what transaction to store. If this field is missing
    /// None of the transction is stored.
    /// "transaction_selector" : {
    ///     "mentions" : \["pubkey-1", "pubkey-2", ..., "pubkey-n"\],
    /// }
    /// The `mentions` field support wildcard to select all transaction or all 'vote' transactions:
    /// For example, to select all transactions:
    /// "transaction_selector" : {
    ///     "mentions" : \["*"\],
    /// }
    /// To select all vote transactions:
    /// "transaction_selector" : {
    ///     "mentions" : \["all_votes"\],
    /// }
    /// # Examples
    ///
    /// {
    ///    "libpath": "/home/solana/target/release/libsolana_geyser_plugin_postgres.so",
    ///    "host": "host_foo",
    ///    "user": "solana",
    ///    "threads": 10,
    ///    "accounts_selector" : {
    ///       "owners" : ["9oT9R5ZyRovSVnt37QvVoBttGpNqR3J7unkb567NP8k3"]
    ///    }
    /// }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        solana_logger::setup_with_default("info");
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );
        let contents = fs::read_to_string(config_file)?;

        let result: serde_json::Value = serde_json::from_str(&contents).unwrap();
        self.accounts_selector = Some(Self::create_accounts_selector_from_config(&result));
        self.transaction_selector = Some(Self::create_transaction_selector_from_config(&result));

        let _config: GeyserPluginPostgresConfig =
            serde_json::from_str(&contents).map_err(|err| {
                GeyserPluginError::ConfigFileReadError {
                    msg: format!(
                        "The config file is not in the JSON format expected: {:?}",
                        err
                    ),
                }
            })?;

        let (sender, receiver) = unbounded::<llm::MLJob>();
        self.job_sender = Some(sender);
        self.worker_thread = Some(Builder::new().name("rpc-worker".to_string()).spawn(move || {
            loop {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(new_job) => {
                        info!("job: {:?}", new_job);
                        let client = reqwest::blocking::Client::builder()
                            .timeout(Duration::from_secs(5))
                            .build();
                        let client = match client {
                            Ok(client) => client,
                            Err(err) => {
                                warn!("client instantiation failed: {}", err);
                                return;
                            }
                        };

                        let body = new_job.parameters;
                        let write_url = "http://127.0.0.1:9100/api/addjob";
                        warn!("posting stuff.. to: {}", write_url);
                        let response = client.post(write_url).body(body).send();
                        if let Ok(resp) = response {
                            let status = resp.status();
                            if !status.is_success() {
                                let text = resp
                                    .text()
                                    .unwrap_or_else(|_| "[text body empty]".to_string());
                                warn!("submit response unsuccessful: {} {}", status, text,);
                            } else {
                                info!("response: {:?}", resp.text());
                            }
                        } else {
                            warn!("submit error: {}", response.unwrap_err());
                        }
                    },
                    Err(RecvTimeoutError::Disconnected) => {
                        break;
                    },
                    Err(e) => {
                    }
                }
            }
        }).unwrap());

        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading plugin: {:?}", self.name());
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        // skip updating account on startup of batch_optimize_by_skiping_older_slots
        // is configured
        if is_startup
            && self
                .batch_starting_slot
                .map(|slot_limit| slot < slot_limit)
                .unwrap_or(false)
        {
            return Ok(());
        }

        let mut measure_all = Measure::start("geyser-plugin-postgres-update-account-main");
        match account {
            ReplicaAccountInfoVersions::V0_0_1(_) => {
                return Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginPostgresError::ReplicaAccountV001NotSupported,
                )));
            }
            ReplicaAccountInfoVersions::V0_0_2(_) => {
                return Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginPostgresError::ReplicaAccountV001NotSupported,
                )));
            }
            ReplicaAccountInfoVersions::V0_0_3(account) => {
                let mut measure_select =
                    Measure::start("geyser-plugin-postgres-update-account-select");
                if let Some(accounts_selector) = &self.accounts_selector {
                    info!("selector?");
                    if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
                measure_select.stop();
                inc_new_counter_debug!(
                    "geyser-plugin-postgres-update-account-select-us",
                    measure_select.as_us() as usize,
                    100000,
                    100000
                );

                if let Ok(llm_account_data) = llm::MLJobPoolState::unpack(account.data) {
                    for job in llm_account_data.jobs {
                        let h = hash(&job.parameters);
                        if !self.jobs.contains_key(&h) {
                            if let Some(sender) = self.job_sender.as_ref() {
                                info!("sending job {:?}", job);
                                if let Err(e) = sender.send(job) {
                                    info!("send error {:?}", e);
                                }
                            }
                        }
                    }
                }

                debug!(
                    "Updating account {:?} with owner {:?} at slot {:?} using account selector {:?}",
                    bs58::encode(account.pubkey).into_string(),
                    bs58::encode(account.owner).into_string(),
                    slot,
                    self.accounts_selector.as_ref().unwrap()
                );

                let mut measure_update =
                    Measure::start("geyser-plugin-postgres-update-account-client");
                measure_update.stop();
            }
        }

        measure_all.stop();

        inc_new_counter_debug!(
            "geyser-plugin-postgres-update-account-main-us",
            measure_all.as_us() as usize,
            100000,
            100000
        );

        Ok(())
    }

    fn update_slot_status(&self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<()> {
        info!("Updating slot {:?} (parent: {:?}) at with status {:?}", slot, parent, status);

        Ok(())
    }

    fn notify_end_of_startup(&self) -> Result<()> {
        info!("Notifying the end of startup for accounts notifications");
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_2(transaction_info) => {
                if let Some(transaction_selector) = &self.transaction_selector {
                    if !transaction_selector.is_transaction_selected(
                        transaction_info.is_vote,
                        Box::new(transaction_info.transaction.message().account_keys().iter()),
                    ) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }

                /*let result = client.log_transaction_info(transaction_info, slot);

                if let Err(err) = result {
                    return Err(GeyserPluginError::SlotStatusUpdateError{
                        msg: format!("Failed to persist the transaction info to the PostgreSQL database. Error: {:?}", err)
                    });
                }*/
            }
            _ => {
                return Err(GeyserPluginError::SlotStatusUpdateError{
                    msg: "Failed to persist the transaction info to the PostgreSQL database. Unsupported format.".to_string()
                });
            }
        }

        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        match block_info {
            ReplicaBlockInfoVersions::V0_0_2(_block_info) => {
                /*let result = client.update_block_metadata(block_info);

                if let Err(err) = result {
                    return Err(GeyserPluginError::SlotStatusUpdateError{
                        msg: format!("Failed to persist the update of block metadata to the PostgreSQL database. Error: {:?}", err)
                    });
                }*/
            }
            ReplicaBlockInfoVersions::V0_0_1(_) => {
                /*return Err(GeyserPluginError::SlotStatusUpdateError{
                    msg: "Failed to persist the transaction info to the PostgreSQL database. Unsupported format.".to_string()
                });*/
            }
        }

        Ok(())
    }

    /// Check if the plugin is interested in account data
    /// Default is true -- if the plugin is not interested in
    /// account data, please return false.
    fn account_data_notifications_enabled(&self) -> bool {
        self.accounts_selector
            .as_ref()
            .map_or_else(|| false, |selector| selector.is_enabled())
    }

    /// Check if the plugin is interested in transaction data
    fn transaction_notifications_enabled(&self) -> bool {
        self.transaction_selector
            .as_ref()
            .map_or_else(|| false, |selector| selector.is_enabled())
    }
}

impl GeyserPluginPostgres {
    fn create_accounts_selector_from_config(config: &serde_json::Value) -> AccountsSelector {
        let accounts_selector = &config["accounts_selector"];

        if accounts_selector.is_null() {
            AccountsSelector::default()
        } else {
            let accounts = &accounts_selector["accounts"];
            let accounts: Vec<String> = if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            info!("accounts: {:?}", accounts);
            let owners = &accounts_selector["owners"];
            let owners: Vec<String> = if owners.is_array() {
                owners
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            AccountsSelector::new(&accounts, &owners)
        }
    }

    fn create_transaction_selector_from_config(config: &serde_json::Value) -> TransactionSelector {
        let transaction_selector = &config["transaction_selector"];

        if transaction_selector.is_null() {
            TransactionSelector::default()
        } else {
            let accounts = &transaction_selector["mentions"];
            let accounts: Vec<String> = if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            TransactionSelector::new(&accounts)
        }
    }

    pub fn new() -> Self {
        Self::default()
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginPostgres pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserPluginPostgres::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

#[cfg(test)]
pub(crate) mod tests {
    use {super::*, serde_json, tempfile::NamedTempFile, std::{io::Write, path::PathBuf}, solana_sdk::pubkey::Pubkey, solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3};

    #[test]
    fn test_accounts_selector_from_config() {
        let config = "{\"accounts_selector\" : { \
           \"owners\" : [\"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin\"] \
        }}";

        let config: serde_json::Value = serde_json::from_str(config).unwrap();
        GeyserPluginPostgres::create_accounts_selector_from_config(&config);
    }

    #[test]
    fn test_library() {
        let mut gp = GeyserPluginPostgres::new();

        let mut temp_file = NamedTempFile::new().unwrap();
        let owner = Pubkey::new_unique();
        let json = format!("{{\"accounts_selector\": {{\"owners\": [\"{owner}\"]}}, \"lib\": \"libllama.dylib\"}}");
        write!(temp_file, "{}", json).unwrap();

        let path = temp_file.path().to_str().unwrap();
        gp.on_load(&path).unwrap();
        let pk = Pubkey::new_unique();
        let mut pool_state = llm::MLJobPoolState::default();
        let mut job = llm::MLJob::default();
        //job.parameters = "{\"stuff\": \"these are my parameters\"}".as_bytes().to_vec();
        job.parameters = "{\"url\": \"http://myjob.blah\"}".as_bytes().to_vec();
        pool_state.jobs.push(job);
        let mut data = [0u8; 1024];
        pool_state.pack(&mut data).unwrap();
        let account = ReplicaAccountInfoV3 {
            pubkey: pk.as_ref(),
            lamports: 1,
            owner: owner.as_ref(),
            executable: false,
            rent_epoch: 0,
            data: &data,
            write_version: 0,
            txn: None,
        };
        let rep_account = ReplicaAccountInfoVersions::V0_0_3(&account);
        gp.update_account(rep_account, 0, false).unwrap();
        std::thread::sleep(Duration::from_secs(5));
        gp.on_unload();
    }
}
