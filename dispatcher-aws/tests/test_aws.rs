use bitvmx_dispatcher_aws::aws_handler::AwsHandler;
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
