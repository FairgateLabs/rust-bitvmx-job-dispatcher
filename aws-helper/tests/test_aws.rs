use bitvmx_aws_helper::aws_handler::{AwsHandler, CommandStatus};
use test_helper::test_helper::init_trace;

#[test]
#[ignore]
fn test_create_instance() {
    init_trace();

    let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
    let aws_handler = AwsHandler::new(config_path).unwrap();
    let instance_id = aws_handler.create_instance("test-instance").unwrap();
    aws_handler.terminate_instance(&instance_id).unwrap();
}

#[test]
#[ignore]
fn test_s3_file() {
    init_trace();

    let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
    let aws_handler = AwsHandler::new(config_path).unwrap();
    let data = b"Hello, S3!".to_vec();
    let key = "test-file.txt";

    aws_handler.upload_file(key, data.clone()).unwrap();
    let downloaded_data = aws_handler.download_file(key).unwrap();
    aws_handler.delete_file(key).unwrap();
    assert_eq!(data, downloaded_data);
}

#[test]
#[ignore]
fn test_wait_running() {
    init_trace();

    let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
    let aws_handler = AwsHandler::new(config_path).unwrap();
    let instance_id = aws_handler.create_instance("test-instance").unwrap();

    let ready = aws_handler
        .wait_for_instance_ready(&instance_id, 300)
        .unwrap();
    aws_handler.terminate_instance(&instance_id).unwrap();
    assert!(ready, "Instance did not become ready within the timeout");
}

#[test]
#[ignore]
fn test_execution() {
    init_trace();

    let config_path = format!("{}/config/config.yaml", env!("CARGO_MANIFEST_DIR"));
    let aws_handler = AwsHandler::new(config_path).unwrap();
    let instance_id = aws_handler.create_instance("test-instance").unwrap();

    let ready = aws_handler
        .wait_for_instance_ready(&instance_id, 300)
        .unwrap();

    let command = vec![
        "echo 'Hello World' > /tmp/hello.txt".to_string(),
        format!(
            "aws s3 cp /tmp/hello.txt s3://{}/successful_test.txt",
            aws_handler.bucket_name()
        )
        .to_string(),
    ];

    let command_id = aws_handler.send_command(&instance_id, command).unwrap();

    let result = aws_handler
        .wait_for_command_finished(&instance_id, &command_id, 300)
        .unwrap();

    let expected_output = "Hello World\n";
    let file_result = aws_handler.download_file("successful_test.txt").unwrap();

    aws_handler.terminate_instance(&instance_id).unwrap();

    aws_handler.delete_file("successful_test.txt").unwrap();

    assert!(
        matches!(result, CommandStatus::Success(_)),
        "Command did not succeed: {:?}",
        result
    );
    assert_eq!(file_result, expected_output.as_bytes());
    assert!(ready, "Instance did not become ready within the timeout");
}
