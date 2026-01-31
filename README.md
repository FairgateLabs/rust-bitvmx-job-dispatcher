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

Prerequisites
- An S3 bucket for input and result files.
- An EC2 instance (avoid Amazon Linux if known incompatible).

Required software on the instance
- Git
- Rust (install via rustup)
- AWS CLI
- All ZKP build dependencies required by the rust-bitvmx-zk-proof repository

IAM and access
- Grant the instance access to the S3 bucket (at minimum s3:GetObject and s3:PutObject for the target bucket).
- Attach the AmazonSSMManagedInstanceCore policy so you can manage the instance with AWS Systems Manager (SSM).

Quick setup outline
1. Launch an EC2 instance with the IAM role attached.
2. Connect and install Git, rustup (Rust), and the AWS CLI.
3. Clone rust-bitvmx-zk-proof and install any ZKP build dependencies and toolchains it requires.
4. Verify the instance can read/write the S3 bucket and that SSM connectivity works (you can use the provided integration test for this).

### Provisioning script
This repository contains a shell script to help bootstrap and configure an AWS dispatcher instance.