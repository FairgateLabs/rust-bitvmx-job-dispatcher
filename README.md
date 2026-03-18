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