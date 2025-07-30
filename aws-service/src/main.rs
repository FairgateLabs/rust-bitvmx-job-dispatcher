use std::env;
use tokio::fs::File;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, SdkConfig};
use aws_sdk_ec2::{Client as EC2Client, Error as EC2Error};
use aws_sdk_ssm::{Client as SsmClient, Error as SsmError};
use aws_sdk_s3::{Client as S3Client, Error as S3Error};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 2 {
        panic!("Too many arguments")
    }

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let (ec2_client, config) = runtime.block_on(create_service()).expect("Failed to run the service");

    let instance_id = "i-087552855f0b1c0f8";

    println!("Starting instance {}", instance_id);
    runtime.block_on(start_instance(&ec2_client, instance_id))
        .expect("Could not start the instance");
    println!("Instance started");

    runtime.block_on(send_command(&config, instance_id, args[1].clone()))
        .expect("Could not send the command");

    runtime.block_on(download_file(&config))
        .expect("Could not download the file");

    println!("File downloaded");

    println!("Stopping instance {}", instance_id);
    runtime.block_on(stop_instance(&ec2_client, instance_id))
        .expect("Could not stop the instance");
    println!("Instance stopped");
}

async fn create_service() -> Result<(EC2Client, SdkConfig), EC2Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let behavior = BehaviorVersion::latest();
    let config = aws_config::defaults(behavior).region(region_provider).load().await;
    let client = EC2Client::new(&config);

    Ok((client, config))
}

async fn start_instance(client: &EC2Client, instance_id: &str) -> Result<(), EC2Error> {
    client
        .start_instances()
        .instance_ids(instance_id)
        .send()
        .await?;

    Ok(())
}

async fn stop_instance(client: &EC2Client, instance_id: &str) -> Result<(), EC2Error> {
    client
        .stop_instances()
        .instance_ids(instance_id)
        .send()
        .await?;

    Ok(())
}

async fn send_command(config: &SdkConfig, instance_id: &str, zkp_to_run: String) -> Result<(), SsmError> {
    let client = SsmClient::new(config);
    let command = "echo 'Hello from Rust SDK' > /tmp/greeting.txt && aws s3 cp /tmp/greeting.txt s3://prueba2025b1/greeting.txt > /tmp/upload.log 2>&1";

    let command = client
        .send_command()
        .instance_ids(instance_id)
        .document_name("AWS-RunShellScript")
        .comment("Create file and upload to S3")
        .parameters(
            "commands",
            vec![command.to_string()]
        )
        .send()
        .await?;

    let command_id = command
        .command()
        .expect("No command received")
        .command_id()
        .expect("No command_id received");

    println!("Command sent. ID: {}", command_id);

    Ok(())
}

async fn download_file(config: &SdkConfig) -> Result<(), S3Error> {
    let client = S3Client::new(&config);

    let bucket = "prueba2025b1";
    let key = "greeting.txt";
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let mut file = File::create("downloaded.txt").await.expect("Could not create the file");
    let mut body = resp.body.into_async_read();
    tokio::io::copy(&mut body, &mut file).await.expect("Could not copy the data to the file");

    Ok(())
}
