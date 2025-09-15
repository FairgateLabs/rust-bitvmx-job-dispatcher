use tracing::error;
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

// To make this example work, you need to:
// 1. go to ../BitVMX-CPU and run cargo build --release
// 2. run the server example first (from bitvmx-broker).
//      cargo run --release --example server -- --port 10000
// 3. Then run the job-dispatcher
//      cargo run --release --bin bitvmx-emulator-dispatcher -- --port 10000 --my-id 1
// 4. Then trigger one execution
//      cargo run --release --example challenge --features "emulator"

pub(crate) struct Paths {
    privk: String,
    yaml_path: String,
    checkpoint_prover_path: String,
    checkpoint_verifier_path: String,
    commands_file: String,
}

impl Paths {
    pub fn new(path_corrector: &str) -> Self {
        Self {
            privk: format!("{}../rust-bitvmx-broker/certs/services.key", path_corrector),
            yaml_path: format!(
                "{}../BitVMX-CPU/docker-riscv32/riscv32/build/hello-world.yaml",
                path_corrector
            ),
            checkpoint_prover_path: format!("{}temp-runs/challenge/42/prover/", path_corrector),
            checkpoint_verifier_path: format!("{}temp-runs/challenge/42/verifier/", path_corrector),
            commands_file: format!("{}temp-runs/commands.json", path_corrector),
        }
    }
}
pub mod emulator {
    use super::Paths;
    use bitvmx_broker::identification::identifier::Identifier;
    use bitvmx_broker::rpc::tls_helper::Cert;
    use bitvmx_broker::rpc::BrokerConfig;
    use bitvmx_broker::{channel::channel::DualChannel, identification::allow_list::AllowList};
    use bitvmx_cpu_definitions::challenge::EmulatorResultType;
    use bitvmx_job_dispatcher::dispatcher_job::{DispatcherJob, ResultMessage};
    use bitvmx_job_dispatcher_types::emulator_messages::EmulatorJobType;
    use emulator::executor::utils::FailConfiguration;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::{fs, net::IpAddr, path::Path, thread::sleep, time::Duration};
    use tracing::info;

    pub(crate) fn run_job(paths: Paths, port: u16) -> Result<(), anyhow::Error> {
        info!("Starting challenge example...");
        let privk = fs::read_to_string(paths.privk)?;
        let my_id = 2;
        let dest_id = 1;
        let cert = Cert::new_with_privk(&privk)?;
        let allow_list =
            AllowList::from_certs(vec![cert.clone()], vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])?;
        let emulator_id = Identifier {
            pubkey_hash: cert.get_pubk_hash()?,
            id: Some(dest_id),
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        };

        let channel = DualChannel::new(
            &BrokerConfig::new(
                port,
                Some(IpAddr::from([127, 0, 0, 1])),
                cert.get_pubk_hash()?,
            )?,
            cert,
            Some(my_id),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 11000),
            Some(allow_list),
        )?;

        let input = 0;
        let input = vec![17, 17, 17, input];
        let yaml_path = paths.yaml_path;
        let checkpoint_prover_path = paths.checkpoint_prover_path;
        let checkpoint_verifier_path = paths.checkpoint_verifier_path;
        let commands_file = paths.commands_file;

        let fail_config_prover = Some(FailConfiguration::new_fail_hash(100));
        let fail_config_verifier = None;
        let force_condition = emulator::decision::challenge::ForceCondition::ValidInputStepAndHash;
        let force_challenge = emulator::decision::challenge::ForceChallenge::No;

        for path in &[
            checkpoint_prover_path.clone(),
            checkpoint_verifier_path.clone(),
        ] {
            if !Path::new(path).exists() {
                fs::create_dir_all(path)?;
            }
        }

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: EmulatorJobType::ProverExecute(
                yaml_path.clone(),
                input.clone(),
                checkpoint_prover_path.clone(),
                commands_file.clone(),
                fail_config_prover.clone(),
            ),
        })?;
        channel.send(emulator_id.clone(), msg)?;

        info!("Starting emulator job...");
        let (prover_result, job_id) = wait_for_result(&channel, "ProverExecuteResult", 10, 1)?;
        let (step, hash, halt) =
            EmulatorResultType::from_value(prover_result)?.as_prover_execute()?;

        info!(
            "✅ Got prover result: step {}, hash {}, halt {:?}",
            step, hash, halt
        );
        info!("with job_id {}", job_id);

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: EmulatorJobType::VerifierCheckExecution(
                yaml_path.clone(),
                input,
                checkpoint_verifier_path.to_string(),
                step,
                hash,
                commands_file.clone(),
                force_condition,
                fail_config_verifier.clone(),
            ),
        })?;
        channel.send(emulator_id.clone(), msg)?;

        let (verifier_result, job_id) =
            wait_for_result(&channel, "VerifierCheckExecutionResult", 10, 1)?;
        let step = EmulatorResultType::from_value(verifier_result)?.as_verifier_check()?;
        info!("✅ Checked verifier result with last step {:?}", step);
        info!("with job_id {}", job_id);

        let mut v_decision = 0;
        let total_rounds = get_total_rounds(&yaml_path);
        for round in 1..total_rounds + 1 {
            let msg = serde_json::to_string(&DispatcherJob {
                job_id: "uid_job".to_string(),
                job_type: EmulatorJobType::ProverGetHashesForRound(
                    yaml_path.clone(),
                    checkpoint_prover_path.clone(),
                    round,
                    v_decision,
                    commands_file.clone(),
                    fail_config_prover.clone(),
                    emulator::decision::nary_search::NArySearchType::ReadValueChallenge,
                ),
            })?;
            channel.send(emulator_id.clone(), msg)?;

            let (prover_hashes_result, job_id) =
                wait_for_result(&channel, "ProverGetHashesForRoundResult", 10, 1)?;
            let (hashes, mut my_round): (Vec<String>, u8) =
                EmulatorResultType::from_value(prover_hashes_result)?.as_prover_hashes()?;
            info!("✅ Got prover hashes: {:?}, round: {:?}", hashes, my_round);
            info!("with job_id {}", job_id);

            let msg = serde_json::to_string(&DispatcherJob {
                job_id: "uid_job".to_string(),
                job_type: EmulatorJobType::VerifierChooseSegment(
                    yaml_path.clone(),
                    checkpoint_verifier_path.clone(),
                    round,
                    hashes,
                    commands_file.clone(),
                    fail_config_verifier.clone(),
                    emulator::decision::nary_search::NArySearchType::ReadValueChallenge,
                ),
            })?;
            channel.send(emulator_id.clone(), msg)?;

            let (verifier_choose_segment_result, job_id) =
                wait_for_result(&channel, "VerifierChooseSegmentResult", 10, 1)?;
            (v_decision, my_round) =
                EmulatorResultType::from_value(verifier_choose_segment_result)?.as_v_decision()?;
            info!(
                "✅ Got verifier choose segment: v_decision {}, round {}",
                v_decision, my_round
            );
            info!("with job_id {}", job_id);
        }

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: EmulatorJobType::ProverFinalTrace(
                yaml_path.clone(),
                checkpoint_prover_path.clone(),
                v_decision + 1,
                commands_file.clone(),
                fail_config_prover.clone(),
            ),
        })?;
        channel.send(emulator_id.clone(), msg)?;

        let (final_trace, job_id) = wait_for_result(&channel, "ProverFinalTraceResult", 10, 1)?;
        let final_trace = EmulatorResultType::from_value(final_trace)?.as_final_trace()?;

        info!("✅ Got prover final trace: {:?}", final_trace);
        info!("with job_id {}", job_id);

        let msg = serde_json::to_string(&DispatcherJob {
            job_id: "uid_job".to_string(),
            job_type: EmulatorJobType::VerifierChooseChallenge(
                yaml_path.clone(),
                checkpoint_verifier_path.clone(),
                final_trace,
                commands_file.clone(),
                fail_config_verifier.clone(),
                force_challenge,
            ),
        })?;
        channel.send(emulator_id, msg)?;

        let (result, job_id) = wait_for_result(&channel, "VerifierChooseChallengeResult", 10, 1)?;
        let challenge = EmulatorResultType::from_value(result)?.as_challenge()?;
        info!("✅ Got verifier choose challenge: {:?}", challenge);
        info!("with job_id {}", job_id);
        Ok(())
    }

    fn wait_for_result(
        channel: &DualChannel,
        expected_type: &str,
        max_attempts: usize,
        delay_secs: u64,
    ) -> Result<(serde_json::Value, String), anyhow::Error> {
        for _ in 0..max_attempts {
            if let Some((msg, _from)) = channel.recv()? {
                info!("Received message: {}", msg);
                let msg = serde_json::from_str::<ResultMessage>(&msg)?;
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&msg.result) {
                    if json["type"] == expected_type {
                        return Ok((json, msg.job_id));
                    }
                } else {
                    info!("Received unstructured result: {}", msg.result);
                }
            } else {
                info!("Waiting result execution");
                sleep(Duration::from_secs(delay_secs));
            }
        }

        Err(anyhow::anyhow!(
            "Timeout: did not receive expected result '{}'",
            expected_type
        ))
    }

    pub fn get_total_rounds(pdf: &str) -> u8 {
        //TODO: duplicate function only for testing
        let config = config::Config::builder()
            .add_source(config::File::with_name(pdf))
            .build()
            .unwrap();
        let aprox_max_steps = config.get::<u64>("max_steps").unwrap();
        let nary = config.get::<u8>("nary_search").unwrap();
        let max_bits = f64::ceil(f64::log2(aprox_max_steps as f64));
        let nary_bits = f64::log2(nary as f64);
        let full_rounds = f64::floor(max_bits / nary_bits);
        let bits_left = max_bits - full_rounds * nary_bits;
        let nary_last_round = if bits_left as u8 == 0 {
            0
        } else {
            f64::powf(2.0, bits_left) as u8
        };
        full_rounds as u8 + if nary_last_round > 0 { 1 } else { 0 }
    }
}

#[allow(dead_code)]
fn main() {
    init_trace().unwrap();
    let paths = Paths::new("");
    if let Err(e) = emulator::run_job(paths, 10000) {
        error!("Error: {}", e);
    }
}

fn init_trace() -> Result<(), anyhow::Error> {
    let filter = EnvFilter::builder()
        .parse("info,tarpc=off") // Include everything at "info"
        .expect("Invalid filter");

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .try_init()?;
    Ok(())
}
