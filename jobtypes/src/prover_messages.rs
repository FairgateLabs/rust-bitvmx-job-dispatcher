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
                    format!(
                        "sh {} {} {} {}",
                        shell_quote(&format!("{output_file_path}/prove.sh")),
                        shell_quote(&input_file),
                        shell_quote(elf),
                        shell_quote(output_file_path)
                    ),
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

/// Quotes a value so the shell passes it as one argument, whatever it contains: the paths come from the job.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProverJobType {
    Prove(Vec<u8>, String, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path, process::Command};

    fn job_with_elf(elf: &str) -> ProverJobType {
        ProverJobType::Prove(vec![1, 2, 3], elf.to_string(), "out".to_string())
    }

    fn job() -> ProverJobType {
        job_with_elf("guest.bin")
    }

    #[test]
    fn prove_runs_the_script_with_the_job_paths() {
        let (cmd, args, result_file, _) = job().command().unwrap();
        assert_eq!(cmd, "sh");
        assert_eq!(
            args,
            vec!["-c", "sh 'out/prove.sh' 'out/input.bin' 'guest.bin' 'out'"]
        );
        assert_eq!(result_file, "out/output.json");
    }

    #[test]
    fn shell_quote_keeps_any_value_as_one_argument() {
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("x; rm -rf /"), "'x; rm -rf /'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
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

    struct Machine {
        /// The image has the cuda build of the prover.
        gpu_build: bool,
        /// `nvidia-smi` finds a GPU.
        gpu_answers: bool,
        /// The stark fails.
        stark_fails: bool,
    }

    struct Run {
        success: bool,
        /// Each line is how one prover binary was called: "<binary> <arguments>".
        calls: Vec<String>,
        /// Files in the directory where the job runs, after it ran.
        files: Vec<String>,
    }

    /// Runs the command of the job against fake prover binaries and a fake `nvidia-smi`.
    fn run_job(test_name: &str, job: ProverJobType, machine: Machine) -> Run {
        let root =
            std::env::temp_dir().join(format!("prove-sh-{test_name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let repo = root.join("rust-bitvmx-zk-proof");
        let bin = repo.join("target/release");
        let tools = root.join("tools");
        fs::create_dir_all(&bin).unwrap();
        fs::create_dir_all(repo.join("out")).unwrap();
        fs::create_dir_all(&tools).unwrap();
        fs::write(repo.join("out/prove.sh"), PROVE_SCRIPT).unwrap();

        let log = root.join("calls.log");
        let stark_exit = if machine.stark_fails { 3 } else { 0 };
        let prover = format!(
            "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> '{}'\n[ \"$1\" = prove-stark ] && exit {stark_exit}\nexit 0\n",
            log.display()
        );
        write_executable(&bin.join("host"), &prover);
        if machine.gpu_build {
            write_executable(&bin.join("host-gpu"), &prover);
        }
        let gpu_exit = if machine.gpu_answers { 0 } else { 1 };
        write_executable(
            &tools.join("nvidia-smi"),
            &format!("#!/bin/sh\nexit {gpu_exit}\n"),
        );

        let (_, args, _, _) = job.command().unwrap();
        let path = format!("{}:{}", tools.display(), std::env::var("PATH").unwrap());
        let status = Command::new("sh")
            .args(&args)
            .current_dir(&repo)
            .env("PATH", path)
            .env("PREFETCH_DIR", root.join("no-containerd"))
            .status()
            .unwrap();

        let calls = fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .collect();
        let files = fs::read_dir(&repo)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        fs::remove_dir_all(&root).unwrap();
        Run {
            success: status.success(),
            calls,
            files,
        }
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, content: &str) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, content).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    const SNARK_CALL: &str =
        "host prove-snark --input out/stark_proof.bin --json out/output.json --json-input out/stark.json";

    #[test]
    fn prove_uses_the_gpu_build_for_the_stark_when_a_gpu_answers() {
        let run = run_job(
            "gpu",
            job(),
            Machine {
                gpu_build: true,
                gpu_answers: true,
                stark_fails: false,
            },
        );
        assert!(run.success);
        assert_eq!(
            run.calls,
            vec![
                "host-gpu prove-stark --input out/input.bin --elf guest.bin --output out/stark_proof.bin --json out/stark.json",
                SNARK_CALL,
            ]
        );
    }

    #[test]
    fn prove_uses_host_when_the_gpu_does_not_answer() {
        let run = run_job(
            "no-gpu",
            job(),
            Machine {
                gpu_build: true,
                gpu_answers: false,
                stark_fails: false,
            },
        );
        assert!(run.success);
        assert!(run.calls[0].starts_with("host prove-stark"));
        assert_eq!(run.calls[1], SNARK_CALL);
    }

    #[test]
    fn prove_uses_host_without_the_gpu_build() {
        let run = run_job(
            "cpu",
            job(),
            Machine {
                gpu_build: false,
                gpu_answers: true,
                stark_fails: false,
            },
        );
        assert!(run.success);
        assert!(run.calls[0].starts_with("host prove-stark"));
        assert_eq!(run.calls[1], SNARK_CALL);
    }

    #[test]
    fn prove_stops_when_the_stark_fails() {
        let run = run_job(
            "stark-fails",
            job(),
            Machine {
                gpu_build: false,
                gpu_answers: false,
                stark_fails: true,
            },
        );
        assert!(!run.success);
        assert_eq!(run.calls.len(), 1);
        assert!(run.calls[0].starts_with("host prove-stark"));
    }

    #[test]
    fn prove_passes_the_elf_as_one_argument() {
        let run = run_job(
            "quoting",
            job_with_elf("guest; touch injected"),
            Machine {
                gpu_build: false,
                gpu_answers: false,
                stark_fails: false,
            },
        );
        assert!(run.success);
        assert!(run.calls[0].contains("--elf guest; touch injected --output"));
        assert!(!run.files.contains(&"injected".to_string()));
    }
}
