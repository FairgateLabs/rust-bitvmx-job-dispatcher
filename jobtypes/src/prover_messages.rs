use bitvmx_job_dispatcher::{
    dispatcher_error::DispatcherError, dispatcher_message::DispatcherMessage,
};
use serde::{Deserialize, Serialize};

impl DispatcherMessage for ProverJobType {
    fn prepare_local_input(&self) -> Result<(), DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _, output_file_path) => {
                std::fs::create_dir_all(output_file_path)?;
                let input_file = format!("{output_file_path}/input.bin");
                std::fs::write(&input_file, input_value)?;
                std::fs::write(format!("{output_file_path}/prove.sh"), PROVE_SCRIPT)?;
            }
        }
        Ok(())
    }

    fn prepare_remote_input(&self) -> Result<Vec<(Vec<u8>, String, String)>, DispatcherError> {
        match self {
            ProverJobType::Prove(input_value, _, output_file_path) => Ok(vec![
                (
                    input_value.clone(),
                    "input.bin".to_string(),
                    format!("{output_file_path}/input.bin"),
                ),
                (
                    PROVE_SCRIPT.as_bytes().to_vec(),
                    "prove.sh".to_string(),
                    format!("{output_file_path}/prove.sh"),
                ),
            ]),
        }
    }

    fn command(&self) -> Result<(String, Vec<String>, String, String), DispatcherError> {
        match self {
            ProverJobType::Prove(_input_value, elf, output_file_path) => {
                let input_file = format!("{output_file_path}/input.bin");
                let json = format!("{output_file_path}/output.json");
                let cmd = "sh".to_string();
                // A single line: on aws each element of the command is one line of the SSM script.
                let args = vec![
                    "-c".to_string(),
                    format!("sh {output_file_path}/prove.sh {input_file} {elf} {output_file_path}"),
                ];
                Ok((cmd, args, json, "".to_string()))
            }
        }
    }

    fn message_type(&self) -> String {
        match self {
            ProverJobType::Prove(..) => "ProveResult".to_string(),
        }
    }
}

/// Runs the stark and the snark on the machine of the job, see `scripts/prove.sh`. It is sent with the input
/// of each job, so the image only needs the prover binaries.
const PROVE_SCRIPT: &str = include_str!("../scripts/prove.sh");

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path, process::Command};

    fn job() -> ProverJobType {
        ProverJobType::Prove(vec![1, 2, 3], "guest.bin".to_string(), "out".to_string())
    }

    #[test]
    fn prove_runs_the_script_with_the_job_paths() {
        let (cmd, args, result_file, _) = job().command().unwrap();
        assert_eq!(cmd, "sh");
        assert_eq!(
            args,
            vec!["-c", "sh out/prove.sh out/input.bin guest.bin out"]
        );
        assert_eq!(result_file, "out/output.json");
    }

    #[test]
    fn prove_sends_the_script_with_the_input() {
        let files = job().prepare_remote_input().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            files[0],
            (
                vec![1, 2, 3],
                "input.bin".to_string(),
                "out/input.bin".to_string()
            )
        );
        assert_eq!(files[1].0, PROVE_SCRIPT.as_bytes());
        assert_eq!(files[1].1, "prove.sh");
        assert_eq!(files[1].2, "out/prove.sh");
    }

    #[test]
    fn prove_script_has_valid_syntax() {
        let status = Command::new("sh")
            .args(["-n", "scripts/prove.sh"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Runs prove.sh against fake prover binaries that log how they were called.
    fn run_prove_script(test_name: &str, with_gpu_build: bool) -> Vec<String> {
        let root =
            std::env::temp_dir().join(format!("prove-sh-{test_name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let repo = root.join("rust-bitvmx-zk-proof");
        let bin = repo.join("target/release");
        fs::create_dir_all(&bin).unwrap();
        fs::create_dir_all(repo.join("out")).unwrap();
        fs::write(repo.join("out/prove.sh"), PROVE_SCRIPT).unwrap();

        let log = root.join("calls.log");
        let fake = format!(
            "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> {}\n",
            log.display()
        );
        let mut binaries = vec!["host"];
        if with_gpu_build {
            binaries.push("host-gpu");
        }
        for name in binaries {
            write_executable(&bin.join(name), &fake);
        }

        let status = Command::new("sh")
            .args(["-c", "sh out/prove.sh out/input.bin guest.bin out"])
            .current_dir(&repo)
            .env("PREFETCH_DIR", root.join("no-containerd"))
            .status()
            .unwrap();
        assert!(status.success());

        let calls = fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        fs::remove_dir_all(&root).unwrap();
        calls
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, content: &str) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, content).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn prove_script_uses_the_gpu_build_for_the_stark_when_present() {
        let calls = run_prove_script("gpu", true);
        assert_eq!(
            calls,
            vec![
                "host-gpu prove-stark --input out/input.bin --elf guest.bin --output out/stark_proof.bin --json out/stark.json",
                "host prove-snark --input out/stark_proof.bin --json out/output.json --json-input out/stark.json",
            ]
        );
    }

    #[test]
    fn prove_script_falls_back_to_host_without_the_gpu_build() {
        let calls = run_prove_script("cpu", false);
        assert_eq!(calls[0].split(' ').next(), Some("host"));
        assert!(calls[0].contains("prove-stark"));
        assert!(calls[1].starts_with("host prove-snark"));
    }
}
