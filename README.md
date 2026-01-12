# Job Dispatched by BitVMX

## Run the Job Dispatcher Emulator 

To run the Job Dispatcher Emulator, use the following command:

```bash
cargo run --bin job-dispatcher-emulator --port <PORT_NUMBER> --storage-path <STORAGE_PATH>
``` 

Replace `<PORT_NUMBER>` and `<STORAGE_PATH>` with appropriate values for your setup.
- `<PORT_NUMBER>`: The port on which the BitVMX client will listen.
- `<STORAGE_PATH>`: The path to the job dispatcher storage directory. It should be a valid directory path and different for each job dispatcher instance.