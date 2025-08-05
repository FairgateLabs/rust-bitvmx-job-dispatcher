use std::env;
use tokio::fs::File;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, SdkConfig};
use aws_sdk_ec2::{Client as Ec2Client, Error as EC2Error};
use aws_sdk_ssm::{Client as SsmClient, Error as SsmError};
use aws_sdk_s3::{Client as S3Client, Error as S3Error};

//TODO: Personalized Errors
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() > 3 {
        panic!("Too many arguments")
    }

    let instance_ids = load_config(args[2].clone());
    
    if instance_ids.is_empty() {
        panic!("No instance IDs provided");
    }

    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let (ec2_client, config) = runtime.block_on(create_service()).expect("Failed to run the service");

    let client = SsmClient::new(&config);
    let mut can_run_command = false;

    let mut free_instance_id = "";
    
    while !can_run_command {
        for instance_id in &instance_ids {
            can_run_command = runtime.block_on(is_instance_stopped(&ec2_client, instance_id))
                .expect("Could not check if the instance is stopped");

            if can_run_command {
                free_instance_id = instance_id;
                println!("Using instance: {}", free_instance_id);
                break;
            }
        }

        if !can_run_command {
            println!("Waiting for the instance to be free...");
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    }

    println!("Starting instance {}", free_instance_id);
    runtime.block_on(start_instance(&ec2_client, free_instance_id))
        .expect("Could not start the instance");
    println!("Instance started");

    runtime.block_on(send_command(&client, free_instance_id, args[1].clone()))
        .expect("Could not send the command");

    runtime.block_on(download_file(&config))
        .expect("Could not download the file");

    println!("File downloaded");

    println!("Stopping instance {}", free_instance_id);
    runtime.block_on(stop_instance(&ec2_client, free_instance_id))
        .expect("Could not stop the instance");
    println!("Instance stopped");
}

pub async fn is_instance_stopped(ec2: &Ec2Client, instance_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Checking if instance {} is stopped...", instance_id);
    let resp = ec2
        .describe_instances()
        .instance_ids(instance_id)
        .send()
        .await?;

    let state = resp
        .reservations()
        .first()
        .unwrap()
        .instances()
        .first()
        .unwrap()
        .state();

    match state {
        Some(s) => {
            let name = s.name().unwrap().as_str();
            if name == "stopped" {
                println!("Instance is stopped, ready to run command");
                return Ok(true);
            } else if name == "shutting-down" || name == "terminated" {
                println!("Instance is shutting down or terminated, cannot run command");
                return Ok(false);
            } else {
                println!("Instance is not stopped yet, current state: {name}");
                return Ok(false);
            }
        }

        None => {
            println!("Instance state is unknown");
            return Ok(false);
        }
        
    }

}

async fn create_service() -> Result<(Ec2Client, SdkConfig), EC2Error> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let behavior = BehaviorVersion::latest();
    let config = aws_config::defaults(behavior).region(region_provider).load().await;
    let client = Ec2Client::new(&config);

    Ok((client, config))
}

async fn start_instance(client: &Ec2Client, instance_id: &str) -> Result<(), EC2Error> {
    client
        .start_instances()
        .instance_ids(instance_id)
        .send()
        .await?;

    Ok(())
}

async fn stop_instance(client: &Ec2Client, instance_id: &str) -> Result<(), EC2Error> {
    client
        .stop_instances()
        .instance_ids(instance_id)
        .send()
        .await?;

    Ok(())
}

async fn send_command(client: &SsmClient, instance_id: &str, zkp_to_run: String) -> Result<(), SsmError> {  
    let command_to_send = "echo 'Hello from Rust SDK' > /tmp/greeting.txt && aws s3 cp /tmp/greeting.txt s3://prueba2025b1/greeting.txt > /tmp/upload.log 2>&1";
    let command = client
        .send_command()
        .instance_ids(instance_id)
        .document_name("AWS-RunShellScript")
        .comment("Create file and upload to S3")
        .parameters(
            "commands",
            vec![command_to_send.to_string()]
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

fn load_config(config_path: String) -> Vec<String> {
    let file = std::fs::File::open(config_path).expect("Could not open config file");
    let reader = std::io::BufReader::new(file);
    let config: serde_json::Value = serde_json::from_reader(reader).expect("Could not parse config file");

    if let Some(instance_ids) = config.get("instance_ids") {
        instance_ids.as_array()
            .expect("instance_ids should be an array")
            .iter()
            .filter_map(|id| id.as_str().map(String::from))
            .collect()
    } else {
        panic!("No instance_ids found in the config file");
    }
}
