# Job Dispatched by BitVMX

## Run the Job Dispatcher Emulator

To start the Job Dispatcher Emulator, run:

```bash
cargo run --bin job-dispatcher-emulator --port <PORT_NUMBER> --storage-path <STORAGE_PATH>
```

Replace the placeholders:
- `<PORT_NUMBER>`: TCP port the emulator will listen on (numeric).
- `<STORAGE_PATH>`: Filesystem path where dispatcher data will be stored. Use a distinct directory for each dispatcher instance.

Notes:
- Ensure the chosen port is available and allowed by your firewall.
- The storage path must be writable by the process and persist between runs if you want to retain state.

## AWS instance setup

This project can offload work to an AWS EC2 instance. The following describes the recommended instance preparation.

To use this dispatcher, you will need to create an S3 bucket, to configure an EC2 Instance (explained below) and to create an image from that instance.

Required software on the instance
- Git
- Rust (install via rustup)
- AWS CLI
- All ZKP build dependencies required by the rust-bitvmx-zk-proof repository

IAM and access
- Grant the instance access to the S3 bucket (at minimum s3:GetObject and s3:PutObject for the target bucket).
- Attach the AmazonSSMManagedInstanceCore policy so you can manage the instance with AWS Systems Manager (SSM).
This instance_profile_arn of IAM role must be put on the configuration file

Quick setup outline
1. Launch an EC2 instance.
2. Connect and install Git, rustup (Rust), and the AWS CLI.
3. Clone rust-bitvmx-zk-proof and install any ZKP build dependencies and toolchains it requires.
4. Create the Image (Select EC2 Instance, Actions -> Images and Templates -> Create Image)

### Provisioning script
This repository contains a shell script to help install AWS CLI
Also, rust-bitvmx-zk-proof has their own scripts to help install RiscZero and the dependencies

### GPU image

The prove job runs `jobtypes/scripts/prove.sh`, which is uploaded with the input of each job. It runs the stark with
`host-gpu` when the image has it and `nvidia-smi` finds a GPU, otherwise with `host`, and the snark always with `host`. This lets one image use a build with cuda for the stark (about 150 times faster than cpu) while the snark keeps
running on cpu: the cuda groth16 prover crashes with `an illegal memory access was encountered`.

An image for a T4 GPU (g4dn) needs, on top of the setup above:
- The NVIDIA driver (`ubuntu-drivers install`, then reboot) and the CUDA toolkit 12.9.
- `protobuf-compiler`, `clang`, `libclang-dev` and `cmake`, needed to build with cuda.
- `host-gpu`: `host` built with `cargo build --release -p host --features risc0-zkvm/cuda` on a machine with the GPU,
  since the kernels are built for the local architecture (`-arch=native`), and renamed to `host-gpu`. The cpu build stays
  as `host`.
- The guest `.bin` in a fixed path, because the job only uploads its input and the script, and `elf` is a path inside
  the instance.
- The docker image of the groth16 prover (`risczero/risc0-groth16-prover`) loaded in containerd. A volume restored from the
  AMI snapshot reads each block from S3 the first time, at about 5 MB/s per read stream, so the first snark of an instance
  waits about 8 minutes for the proving key. `prove.sh` reads those files in parallel before the stark (about 80 seconds) by
  looking for `stark_verify_final.pk.dmp`, `stark_verify.cs` and `stark_verify` under `/var/lib/containerd`.

Optional settings in `aws-helper/config/config.yaml`:
- `ec2.instance_ready_timeout_secs`: how long a launched instance has to become ready (running and with its status
  checks ok). After that the dispatcher terminates it and fails the job. Default 360; instances usually take 3 to 4
  minutes.
- `ssm.execution_timeout_secs`: SSM `executionTimeout` for each job command. When unset SSM applies its default of
  1 hour, which is shorter than the snark of a large input on cpu.
