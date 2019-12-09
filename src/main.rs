extern crate libc;
extern crate libloading;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate pretty_env_logger as env_logger;
extern crate failure;
extern crate named_type;
#[macro_use]
extern crate named_type_derive;

pub(crate) mod domain;

use failure::*;
use futures::*;
use futures::stream;
use futures::stream::Stream;
use futures::future::ok;
use futures::future::err;
use indyrs::*;
use indyrs::did::create_and_store_my_did;
use indyrs::wallet::create_wallet;
use tokio::runtime::current_thread::Runtime;
use tokio::prelude::*;
use std::{thread, env};
use indyrs::ErrorCode::WalletAlreadyExistsError;
use std::ffi::CString;
use libc::c_char;
use indyrs::{did, ErrorCode, IndyError, pairwise, wallet};
use std::fs::File;
use crate::domain::{WalletCredentials, WalletConfig, AppConfig};
use std::ops::Range;


/// A trait for getting the name of a type
pub trait NamedType {
    /// Returns the canonical name with the fully qualified module name for the
    /// given type
    fn type_name() -> &'static str
        where
            Self: Sized;

    /// Returns a user-friendly short name for the given type
    fn short_type_name() -> &'static str
        where
            Self: Sized;
}


#[derive(Deserialize, Debug, NamedType)]
pub struct Pairwise {
    pub my_did: String,
    pub their_did: String,
    pub metadata: String,
}

#[derive(Deserialize, Debug)]
pub struct PairwiseInfo {
    pub my_did: String,
    pub metadata: String,
}


// config
//  url
//  tls
//  max_connections
//  min_idle_time
//  connection_timeout
//  wallet_scheme

// credentials
//  account
//  password
//  admin_account
//  admin_password

pub const PG_TEST_WALLET: &'static str = r#"{"id":"patriktest5", "storage_type":"postgres_storage", "storage_config": {"url":"localhost:5432"} }"#;
pub const PW_TEST_WALLET_CREDS: &'static str = r#"{"key":"123", "storage_credentials": {"account": "postgres", "password": "mysecretpassword", "admin_account": "postgres", "admin_password": "mysecretpassword"} }"#;

pub const PG_FW_WALLET: &'static str = r#"{"id":"forward_agent_wallet_id", "storage_type":"postgres_storage", "storage_config": {"url":"localhost:5432"} }"#;
pub const PW_FW_WALLET_CREDS: &'static str = r#"{"key":"forward_agent_wallet_passphrase", "storage_credentials": {"account": "postgres", "password": "mysecretpassword", "admin_account": "postgres", "admin_password": "mysecretpassword"} }"#;

pub const INIT_CONFIG: &'static str = r#"{"url":"localhost:5432"}"#;
pub const INIT_CREDENTIALS: &'static str = r#"{"account": "postgres", "password": "mysecretpassword", "admin_account": "postgres", "admin_password": "mysecretpassword"}"#;

pub const RETRIEVE_RECORD_OPTIONS: &'static str = r#"{ "retrieveType":true, "retrieveValue": true, "retrieveTags": true}"#;
pub const SEARCH_OPTIONS: &'static str = r#"{ "retrieveRecords": true, "retrieveTotalCount": true, "retrieveType": true, "retrieveValue": true, "retrieveTags": true}"#;


pub type BoxedFuture<I, E> = Box<Future<Item=I, Error=E>>;

pub trait FutureExt: Future + Sized {
    /// Box this future. Similar to `boxed` combinator, but does not require
    /// the future to implement `Send`.
    fn into_box(self) -> BoxedFuture<Self::Item, Self::Error>;
}

impl<F: Future + 'static> FutureExt for F {
    fn into_box(self) -> Box<Future<Item=Self::Item, Error=Self::Error>> {
        Box::new(self)
    }
}


pub fn get_postgres_storage_plugin() -> String {
    let os = os_type::current_platform();
    let osfile = match os.os_type {
        os_type::OSType::OSX => "/usr/local/lib/libindystrgpostgres.dylib",
        _ => "/usr/lib/libindystrgpostgres.so"
    };
    return osfile.to_owned();
}

fn library_function() -> Box<dyn Future<Item=i32, Error=()>> {
    Box::new(ok((23)))
}


#[cfg(all(unix, test))]
fn _load_lib(library: &str) -> libloading::Result<libloading::Library> {
    libloading::os::unix::Library::open(Some(library), ::libc::RTLD_NOW | ::libc::RTLD_NODELETE)
        .map(libloading::Library::from)
}

#[cfg(any(not(unix), not(test)))]
fn _load_lib(library: &str) -> libloading::Result<libloading::Library> {
    libloading::Library::new(library)
}

pub fn load_storage_library(library: &str, initializer: &str, storage_config: &str, storage_credentials: &str) -> Result<(), ()> {
    debug!("Loading dynamic library {:?}", library);
    let lib_res = _load_lib(library);
    match lib_res {
        Ok(lib) => {
            unsafe {
                debug!("Initializing the dynamic library calling initializer function {:?}", initializer);
                let init_func: libloading::Symbol<unsafe extern fn() -> ErrorCode> = lib.get(initializer.as_bytes()).unwrap();

                match init_func() {
                    ErrorCode::Success => println!("Plugin has been loaded: \"{}\"", library),
                    _ => return Err(println!("Plugin has not been loaded: \"{}\"", library))
                }
//              call the one-time storage init() method to initialize storage
                let init_storage_func: libloading::Symbol<unsafe extern fn(config: *const c_char, credentials: *const c_char) -> ErrorCode> = lib.get("init_storagetype".as_bytes()).unwrap();

                let initConfig = CString::new(storage_config).expect("CString::new failed");
                let initCredentials = CString::new(storage_credentials).expect("CString::new failed");
                let err = init_storage_func(initConfig.as_ptr(), initCredentials.as_ptr());
//
                if err != ErrorCode::Success {
                    return Err(println!("Error init_storage returned an error {:?}", err));
                }
                debug!("Successfully loaded and initialized dynamic library {:?}", library);
            }
        }
        Err(_) => return Err(println!("Plugin has not been loaded: \"{}\"", library))
    }

    Ok(())
}

fn list_pws(wallet_handle: i32) -> Box<Future<Item=(), Error=Error>> {
    future::ok(())
        .and_then(move |_| {
            pairwise::list_pairwise(wallet_handle)
                .map(move |pairwise_list| (pairwise_list, wallet_handle))
                .map_err(|err| err.context("Can't get Forward Agent pairwise list").into())
        })
        .and_then(|(pairwise_list, wallet_handle)| {
            serde_json::from_str::<Vec<String>>(&pairwise_list)
                .map(move |pairwise_list| (pairwise_list, wallet_handle))
                .map_err(|err| err.context("Can't deserialize Forward Agent pairwise list").into())
        })
        .and_then(|(pairwise_list, wallet_handle)| {
            pairwise_list
                .iter()
                .map(|pairwise| serde_json::from_str::<Pairwise>(&pairwise))
                .collect::<Result<Vec<_>, _>>()
                .map(move |pairwise_list| (pairwise_list, wallet_handle))
                .map_err(|err| err.context("Can't deserialize Forward Agent pairwise").into())
        })
        .map(move |(pairwise_list, wallet_handle)| {
            info!("Restoring forward agent connection {:?}", pairwise_list);
            ()
        })
        .into_box()
}


fn search(wallet_handle: i32) -> Box<Future<Item=(), Error=Error>> {
    let wallet_h2 = wallet_handle.clone();
    future::ok(())
        .and_then(move |_| {
            wallet::open_wallet_search(wallet_h2, "Pairwise", r#"{"tagName": {"$like": "*"}}"#, SEARCH_OPTIONS)
        })
        .and_then(move |search_handle| {
            wallet::fetch_wallet_search_next_records(wallet_handle, search_handle, 1000)
        })
        .map_err(|err| err.context("Failed to retrieve search cursor data.").into())
        .map(|res| {
            println!("results = {:?}", res);
        })
        .into_box()
}

fn retrieve_pw_which_doesnt_work(wallet_handle: i32, their_pw: &'static str) -> Box<Future<Item=String, Error=Error>> {
    future::ok(())
        .and_then(move |_| {
            wallet::get_wallet_record(wallet_handle, "Indy::Pairwise", their_pw, RETRIEVE_RECORD_OPTIONS)
        })
        .map(|res| {
            println!("record = {:?}", res);
            res
        })
        .map_err(|err| err.context("Can retrieve wallet record").into())
        .into_box()
}

fn delete_pw(wallet_handle: i32) -> Box<Future<Item=(), Error=Error>> {
    future::ok(())
        .map(move |_| {
//            wallet::get_wallet_record(wallet_handle, "Indy::Pairwise", "XjU1Jz4EDzhe1aB5pKvWGL", RETRIEVE_RECORD_OPTIONS);
            wallet::delete_wallet_record(wallet_handle, "Indy::Pairwise", "6Uj2FpPwopUAd1gbZMjiW4");
        })
        .into_box()
}

fn create_did(wh: i32) {
    println!("Creating their DID");
    did::store_their_did(wh, r#"{"did":"XJLE9MeJWrUo9WQhbxUVZA", "verkey":"~PDCcKtoKjzdrmUA13qk1Bf"}"#).wait();
    println!("Created their DID");

    println!("Created our DID and verkey");
    let m = did::create_and_store_my_did(wh, "{}").wait();
    let (did, verkey) = match m {
        Err(_) => {
            panic!("Failed to create and store my did");
        }
        Ok((did, verkey)) => {
            println!("Craeted our did={} verkey={}", did, verkey);
            (did, verkey)
        }
    };

    println!("Creating pairwise");
    let result = pairwise::create_pairwise(wh, "XJLE9MeJWrUo9WQhbxUVZA", &did, Option::from("Nothing but an insects.")).wait();
    match result {
        Err(x) => panic!("Error creating pairwise!! {}", x),
        _ => {
            println!("Created pairwise with success.");
        }
    }
}

fn try_create_wallet(str_wallet_config: &str, str_wallet_credentials: &str) -> Result<(), Error> {
    match create_wallet(&str_wallet_config, &str_wallet_credentials).wait() {
        Ok(x) => {
            info!("Wallet created");
        }
        Err(x) => if x.error_code == ErrorCode::WalletAlreadyExistsError {
            info!("Could not create wallet as it already existed.");
        } else {
            panic!("Problem creating wallet {:?}", x.message);
        }
    };
    Ok(())
}

fn collect_open_wallet_futures(wallet_config: WalletConfig, wallet_credentials: WalletCredentials, walletIds: Vec<String>) {
    info!("Startin batch. Wallet ids = {:?}", walletIds);
    let futuresList: Vec<Box<dyn Future<Item=WalletHandle, Error=IndyError>>> = walletIds
        .iter()
        .enumerate()
        .map(move |(i, walletId)| {
            let mut iter_wallet_config = wallet_config.clone();
            iter_wallet_config.id = walletId.to_owned();
            let iter_wallet_credentials = wallet_credentials.clone();

            info!("Going to try create wallet '{}' with key {}.", &iter_wallet_config.id, &iter_wallet_credentials.key);
            let str_wallet_config = serde_json::to_string(&iter_wallet_config).expect("Failed to serialize wallet config");
            let str_wallet_credentials = serde_json::to_string(&iter_wallet_credentials).expect("Failed to serialize wallet credentials.");

            info!("{}:: Going to open wallet '{}' using config {:?} and credential {:?}.", i, &wallet_config.id, &str_wallet_config, &str_wallet_credentials);
            // these are the futures we generate, esentially the mapping from i32 to Future<_>
            wallet::open_wallet(&str_wallet_config, &str_wallet_credentials)
//                .and_then(|h| {
//                    println!("Wallet opened, has handle {}", h);
//                    Box::new(futures::future::ok(h))
//                })
        })
//        .map(|f| Box::new(f))
        .collect();
//    futuresList;
}


fn create_open_wallet_future(iter_wallet_config: WalletConfig, iter_wallet_credentials: WalletCredentials) -> Box<dyn Future<Item=WalletHandle, Error=IndyError>>
{
    info!("Going to try create wallet '{}' with key {}.", &iter_wallet_config.id, &iter_wallet_credentials.key);
    let str_wallet_config = serde_json::to_string(&iter_wallet_config).expect("Failed to serialize wallet config");
    let str_wallet_credentials = serde_json::to_string(&iter_wallet_credentials).expect("Failed to serialize wallet credentials.");

//   try_create_wallet(&str_wallet_config, &str_wallet_credentials);

    info!("Going to open wallet '{}' using config {:?} and credential {:?}.", &iter_wallet_config.id, &str_wallet_config, &str_wallet_credentials);
    Box::new(futures::future::ok(()) // this is crucial. This makes the future lazy, because it's not by default!!! Though open_wallet is asynchronous and returns future, it's not the way one might expect it to be.
        // Calling open_wallet will actually initiate execution of opening. If you call open_wallet on 1000 wallets and get back 1000 futures, the opening of these 1000 wallets already started, even
        // if you never execute the received future. If you execute the future to get the handle back, it will either wait for the internal indysdk wallet-opening execution to finish, or it will straight away
        // return value because indy-sdk has already opened it in the meantime.
        .and_then(move |_| wallet::open_wallet(&str_wallet_config, &str_wallet_credentials))
    )
}

fn create_and_open_wallets(wallet_config: WalletConfig, wallet_credentials: WalletCredentials, walletIds: Vec<String>) -> Vec<Box<dyn Future<Item=WalletHandle, Error=IndyError>>> {
    info!("create_and_open_wallets :: {:?}", walletIds);
    let futuresList = walletIds
        .iter()
        .enumerate()
        .map(move |(i, walletId)| {
            let mut iter_wallet_config = wallet_config.clone();
            iter_wallet_config.id = walletId.to_owned();
            create_open_wallet_future(iter_wallet_config.clone(), wallet_credentials.clone())
        })
        .collect();
    futuresList
}

fn generate_multiwallet_ids(range: Range<u32>) -> Vec<String> {
    let mut ids: Vec<String> = vec![];
    for x in range {
        ids.push(format!("mutliwallet{}", x))
    }
    return ids;
}

fn do_the_magic(wallet_config: WalletConfig, wallet_credentials: WalletCredentials) {
    let mut runtime = Runtime::new().unwrap();
    let handle = runtime.handle();

    let storage_config = serde_json::to_string(&wallet_config.storage_config).expect("Failed to serialize 'storage_config'.");
    let storage_credentials = serde_json::to_string(&wallet_credentials.storage_credentials).expect("Failed to serialize 'storage_credentials'.");
    info!("Going to load_storage_library");
    match load_storage_library(&get_postgres_storage_plugin(), "postgresstorage_init", &storage_config, &storage_credentials) {
        Err(err) => panic!("Failed to load or initialize storage plugin."),
        _ => {}
    }
    info!("Finished load_storage_library");

    let parallel_cnt = env::var("PARALELL_CNT").expect("Missing ENV variable 'PARALELL_CNT'").parse::<u32>().expect("Can't parse value of 'PARALELL_CNT' as u32");

    let from0 = 0;
    let to0 = 300;
    let futures = create_and_open_wallets(wallet_config.clone(), wallet_credentials.clone(), generate_multiwallet_ids(from0..to0));
    let stream = stream::iter_ok::<_, IndyError>(futures);

//    let stream2 = stream // TODO: lets get this working first without buffering.
//        .for_each(|handle| { // TODO: Why am I getting future here? I though I would wrok with results already here
//            println!("opened wallet. Handle = {}", handle);
//            futures::future::ok(())
//        });

    // works without buffering like this
//        let stream2 = stream
//            .for_each(|handleFuture| {
//                handleFuture.and_then(|handle| {
//                    println!("opened wallet. Handle = {}", handle);
//                    futures::future::ok(())
//                })
//            });

        let stream2 = stream.buffer_unordered(20)
            .for_each(|handle| {
                    println!("opened wallet. Handle = {}", handle);
                    futures::future::ok(())
            });

    let fff = stream2.into_future().and_then(|_| futures::future::ok(())).map_err(|_| ());
    fff.wait(); // TODO: why seem not ot exectue anything?
//    tokio::run(fff);
}

fn do_the_magic_block_manually(wallet_config: WalletConfig, wallet_credentials: WalletCredentials) {
    let mut runtime = Runtime::new().unwrap();
    let handle = runtime.handle();

    let storage_config = serde_json::to_string(&wallet_config.storage_config).expect("Failed to serialize 'storage_config'.");
    let storage_credentials = serde_json::to_string(&wallet_credentials.storage_credentials).expect("Failed to serialize 'storage_credentials'.");
    info!("Going to load_storage_library");
    match load_storage_library(&get_postgres_storage_plugin(), "postgresstorage_init", &storage_config, &storage_credentials) {
        Err(err) => panic!("Failed to load or initialize storage plugin."),
        _ => {}
    }
    info!("Finished load_storage_library");
    let parallel_cnt = env::var("PARALELL_CNT").expect("Missing ENV variable 'PARALELL_CNT'").parse::<u32>().expect("Can't parse value of 'PARALELL_CNT' as u32");

    let mut futures = create_and_open_wallets(wallet_config.clone(), wallet_credentials.clone(), generate_multiwallet_ids(0..50));

    warn!("Waiting fo futures one by one now ...");
    let f1 = futures.pop();
    warn!("Popped future f1");
//    let f2 = futures.pop();
//    warn!("Popped future f2");
//    let f3 = futures.pop();
//    warn!("Popped future f3");
//    let f4 = futures.pop();
//    warn!("Popped future f4");
//    let f5 = futures.pop();
//    warn!("Popped future f5");
//    let f6 = futures.pop();
//    warn!("Popped future f6");
//    let f7 = futures.pop();
//    warn!("Popped future f7");
//    let f8 = futures.pop();
//    warn!("Popped future f8");
//    let f9 = futures.pop();
//    warn!("Popped future f9");
    let h1 = f1.wait();
    warn!("Result handle {:?}", h1);
//    let h2 = f2.wait();
//    warn!("Result handle {:?}", h2);
//    let h3 = f3.wait();
//    warn!("Result handle {:?}", h3);
//    let h4 = f4.wait();
//    warn!("Result handle {:?}", h4);
//    let h5 = f5.wait();
//    warn!("Result handle {:?}", h5);
//    let h6 = f6.wait();
//    warn!("Result handle {:?}", h6);
//    let h7 = f7.wait();
//    warn!("Result handle {:?}", h7);
//    let h8 = f8.wait();
//    warn!("Result handle {:?}", h8);
//    let h9 = f9.wait();
//    warn!("Result handle {:?}", h9);

}

fn do_the_magic_blocking(wallet_config: WalletConfig, wallet_credentials: WalletCredentials) {
    let mut runtime = Runtime::new().unwrap();
    let handle = runtime.handle();

    let storage_config = serde_json::to_string(&wallet_config.storage_config).expect("Failed to serialize 'storage_config'.");
    let storage_credentials = serde_json::to_string(&wallet_credentials.storage_credentials).expect("Failed to serialize 'storage_credentials'.");
    info!("Going to load_storage_library");
    match load_storage_library(&get_postgres_storage_plugin(), "postgresstorage_init", &storage_config, &storage_credentials) {
        Err(err) => panic!("Failed to load or initialize storage plugin."),
        _ => {}
    }
    info!("Finished load_storage_library");
    let parallel_cnt = env::var("PARALELL_CNT").expect("Missing ENV variable 'PARALELL_CNT'").parse::<usize>().expect("Can't parse value of 'PARALELL_CNT' as u32");
    let open_wallets_cnt = env::var("WALLET_CNT").expect("Missing ENV variable 'WALLET_CNT'").parse::<u32>().expect("Can't parse value of 'WALLET_CNT' as u32");

    let futures = create_and_open_wallets(wallet_config.clone(), wallet_credentials.clone(), generate_multiwallet_ids(0..open_wallets_cnt));
    let stream = stream::iter_ok::<_, IndyError>(futures);

    let stream2 = stream
        .buffer_unordered(parallel_cnt)
        .for_each(|_| {
            println!("Resolved stream value",);
            future::ok(())
        });

    stream2.wait();
}


fn do_magic_by_collecting_indy_futures(wallet_config: WalletConfig, wallet_credentials: WalletCredentials) {
    let storage_config = serde_json::to_string(&wallet_config.storage_config).expect("Failed to serialize 'storage_config'.");
    let storage_credentials = serde_json::to_string(&wallet_credentials.storage_credentials).expect("Failed to serialize 'storage_credentials'.");
    info!("Going to load_storage_library");
    match load_storage_library(&get_postgres_storage_plugin(), "postgresstorage_init", &storage_config, &storage_credentials) {
        Err(err) => panic!("Failed to load or initialize storage plugin."),
        _ => {}
    }
    info!("Finished load_storage_library");
    let parallel_cnt = env::var("PARALELL_CNT").expect("Missing ENV variable 'PARALELL_CNT'").parse::<u32>().expect("Can't parse value of 'PARALELL_CNT' as u32");
    create_and_open_wallets(wallet_config.clone(), wallet_credentials.clone(), generate_multiwallet_ids(0..80));
}

fn open_wallet_blocking(wallet_config: WalletConfig, wallet_credentials: WalletCredentials) {
    let storage_config = serde_json::to_string(&wallet_config.storage_config).expect("Failed to serialize 'storage_config'.");
    let storage_credentials = serde_json::to_string(&wallet_credentials.storage_credentials).expect("Failed to serialize 'storage_credentials'.");
    info!("Going to load_storage_library");
    match load_storage_library(&get_postgres_storage_plugin(), "postgresstorage_init", &storage_config, &storage_credentials) {
        Err(err) => panic!("Failed to load or initialize storage plugin."),
        _ => {}
    }
    info!("Finished load_storage_library");
    let str_wallet_config = serde_json::to_string(&wallet_config).expect("Failed to serialize wallet config");
    let str_wallet_credentials = serde_json::to_string(&wallet_credentials).expect("Failed to serialize wallet credentials.");

    let f = wallet::open_wallet(&str_wallet_config, &str_wallet_credentials);
    let handle = f.wait();
    info!("opened wallet = {:?}", handle);
}

fn run(config_path: &str) {
    info!("Running with config; {:?}", config_path);
    let AppConfig {
        wallet_storage
    } = File::open(config_path)
        .context("Can't open config file")
        .and_then(|reader| serde_json::from_reader(reader)
            .context("Can't parse config file"))
        .expect("Invalid configuration file");

//    open_wallet_blocking(wallet_storage.wallet_config, wallet_storage.wallet_credentials);
    do_the_magic_blocking(wallet_storage.wallet_config, wallet_storage.wallet_credentials);
//    do_the_magic_block_manually(wallet_storage.wallet_config, wallet_storage.wallet_credentials);
//    do_magic_by_collecting_indy_futures(wallet_storage.wallet_config, wallet_storage.wallet_credentials);
    // todo: Try to exeute open wallet on tokio - wall that even wok?/? They don't use send Marker -why? Can i even use tokio for indysdk?
}

fn main() {
    env_logger::init();
    indyrs::logger::set_default_logger(None);

    info!("Main");

    let mut args = env::args();
    args.next(); // skip app name

    while let Some(arg) = args.next() {
        match arg.as_str() {
            _ if args.len() == 0 => return run(&arg),
            _ => {
                info!("Too many options")
            }
        }
    }
}
