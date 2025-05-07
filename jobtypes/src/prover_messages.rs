use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for ProverJobType {
    fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _elf, json) => {
                let input_value = u32::from_be_bytes(input_value.as_slice().try_into().unwrap());
                let cmd = "sh".to_string();
                let args = vec![
                    "-c".to_string(),
                    format!(
                        "../rust-bitvmx-zk-proof/target/release/host prove-stark \
                        --input {} \
                        --output \
                        ./stark-proof.bin \
                        --json {} \
                        && ../rust-bitvmx-zk-proof/target/release/host \
                        prove-snark \
                        --input ./stark-proof.bin \
                        --json {}",
                        input_value, json, json
                    ),
                ];
                Ok((cmd, args, json.clone()))
            }
        }
    }

    fn message_type(&self) -> String {
        match self {
            ProverJobType::Prove(..) => "ProveResult".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
}
