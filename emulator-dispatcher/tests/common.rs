use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use bitvmx_broker::{
    channel::channel::DualChannel,
    identification::allow_list::AllowList,
    rpc::{tls_helper::Cert, BrokerConfig},
};
use bitvmx_job_dispatcher::{dispatcher_loop, get_storage_with_path};
use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
use tracing::info;

const PORT: u16 = 10300;

pub fn start_emulator(
    running: Arc<AtomicBool>,
    storage_path: String,
) -> Result<thread::JoinHandle<Result<(), String>>, anyhow::Error> {
    let handle = thread::spawn(move || {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let my_id = 1;
        let privk = fs::read_to_string("test_cert/services.key").unwrap();

        let cert = Cert::new_with_privk(&privk).unwrap();
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
                .unwrap();

        let config = BrokerConfig::new(PORT, Some(IpAddr::from(ip)), cert.get_pubk_hash().unwrap());
        let channel = DualChannel::new(&config, cert, Some(my_id), allow_list).unwrap();

        let check_interval = Duration::from_secs(1);

        let storage = get_storage_with_path(&storage_path).unwrap();
        if let Err(e) =
            dispatcher_loop::<EmulatorJobType>(channel, check_interval, running, storage)
        {
            return Err(format!("dispatcher error: {e}"));
        }
        info!("Dispatcher loop exited normally");

        Err("Expected abrupt end".to_string())
    });

    Ok(handle)
}
