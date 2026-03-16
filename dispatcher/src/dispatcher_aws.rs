use std::rc::Rc;

use bitvmx_aws_helper::aws_handler::AwsHandler;
use bitvmx_broker::identification::identifier::Identifier;
use bitvmx_dispatcher_utils::Msg;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use storage_backend::storage::{KeyValueStore, Storage};
use tracing::info;

use crate::{
    decode_msg, dispatcher_error::DispatcherError, dispatcher_job::DispatcherJob,
    dispatcher_message::DispatcherMessage, dispatcher_storage::DispatcherStorage,
};

pub struct DispatcherAws {
    pub handler: AwsHandler,
    pub storage: DispatcherAwsStorage,
}

impl DispatcherAws {
    pub fn new(
        config_path: String,
        storage: Rc<DispatcherStorage>,
    ) -> Result<Self, DispatcherError> {
        let handler = AwsHandler::new(config_path)?;
        let storage = DispatcherAwsStorage::new(storage);
        Ok(Self { handler, storage })
    }

    fn assign_jobs(&self) -> Result<Option<String>, DispatcherError> {
        // Get all jobs
        let jobs = self.storage.storage.list_jobs()?;
        if jobs.is_empty() {
            return Ok(None);
        }
        // Get all instances
        let instances = self.storage.get_all_instances()?;

        // Get assigned jobs
        let assigned_jobs = instances
            .iter()
            .map(|instance| instance.job_id.clone())
            .collect::<Vec<String>>();

        // Get unassigned jobs
        let unassigned_jobs = jobs
            .into_iter()
            .filter(|job_id| !assigned_jobs.contains(job_id))
            .collect::<Vec<String>>();

        if unassigned_jobs.is_empty() {
            return Ok(None);
        }

        info!(
            "Found {} unassigned jobs and {} instances",
            unassigned_jobs.len(),
            instances.len()
        );

        if instances.len() < self.handler.get_max_running_instances() {
            info!("Need to spawn new instance");
            return Ok(Some(unassigned_jobs[0].clone()));
        } else {
            info!("Reached max running instances limit");
        }

        Ok(None)
    }

    fn get_job<T>(&self, job_id: &str) -> Result<(DispatcherJob<T>, Identifier), DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let msg = self
            .storage
            .storage
            .get_job(job_id)?
            .ok_or(DispatcherError::JobIdNotFound(job_id.to_string()))?;
        let msg = Msg::from_string(&msg)?;
        let job: DispatcherJob<T> = decode_msg(&msg.raw)?;
        Ok((job, msg.id))
    }

    fn execute_job<T>(&self) -> Result<bool, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let instances = self
            .storage
            .get_all_instances()?
            .iter()
            .filter(|instance| instance.status == InstanceStatus::Init)
            .cloned()
            .collect::<Vec<InstanceInfo>>();
        for mut instance in instances {
            if self.handler.is_instance_ready(&instance.instance_id)? {
                info!(
                    "Executing job {} on instance {}",
                    instance.job_id, instance.instance_id
                );
                let (job, _) = self.get_job::<T>(&instance.job_id)?;
                let command = job.job_type().command()?;
                let mut full_command = vec![command.0.clone()];
                full_command.extend(command.1.clone());
                let command_id = self
                    .handler
                    .send_command(&instance.instance_id, full_command)?;

                info!(
                    "Sent command to instance {}, command id: {}",
                    instance.instance_id, command_id
                );

                instance.command_id = Some(command_id);
                instance.status = InstanceStatus::Running;

                self.storage
                    .save_instance(&instance.instance_id.clone(), &instance)?;
            }
        }

        Ok(false)
    }

    fn complete_jobs<T>(&self) -> Result<bool, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        let instances = self
            .storage
            .get_all_instances()?
            .iter()
            .filter(|instance| instance.status == InstanceStatus::Running)
            .cloned()
            .collect::<Vec<InstanceInfo>>();

        for instance in instances {
            if let Some(command_id) = &instance.command_id {
                if self
                    .handler
                    .is_command_finished(&instance.instance_id, command_id)?
                {
                    info!(
                        "Job {} on instance {} completed",
                        instance.job_id, instance.instance_id
                    );

                    let (_job, id) = self.get_job::<T>(&instance.job_id)?;
                    //TODO: get results

                    self.storage
                        .storage
                        .complete_job(&instance.job_id, ("completed".to_string(), id))?;
                    self.handler.terminate_instance(&instance.instance_id)?;
                }
            }
        }

        Ok(false)
    }

    pub fn tick<T>(&self) -> Result<bool, DispatcherError>
    where
        T: DispatcherMessage + DeserializeOwned,
    {
        if let Some(job_id) = self.assign_jobs()? {
            info!("Spawning new instance for job {}", job_id);
            let instance_id = self
                .handler
                .create_instance(&format!("job-id-{}", job_id))?;
            self.storage.save_instance(
                &instance_id,
                &InstanceInfo::new(instance_id.clone(), InstanceStatus::Init, job_id.clone()),
            )?;
        }

        self.execute_job::<T>()?;
        self.complete_jobs::<T>()?;

        Ok(false)
    }
}

pub struct DispatcherAwsStorage {
    storage: Rc<DispatcherStorage>,
}

fn instance_key(instance_id: &str) -> String {
    format!("instance_{}", instance_id)
}

fn instances_key() -> String {
    "instances".to_string()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InstanceStatus {
    Init,
    Running,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceInfo {
    instance_id: String,
    status: InstanceStatus,
    job_id: String,
    command_id: Option<String>,
}

impl InstanceInfo {
    pub fn new(instance_id: String, status: InstanceStatus, job_id: String) -> Self {
        Self {
            instance_id,
            status,
            job_id,
            command_id: None,
        }
    }
}

impl DispatcherAwsStorage {
    pub fn new(storage: Rc<DispatcherStorage>) -> Self {
        Self { storage }
    }

    fn db(&self) -> &Rc<Storage> {
        &self.storage.storage
    }

    pub fn get_instances(&self) -> Result<Vec<String>, DispatcherError> {
        Ok(self.db().get(&instances_key())?.unwrap_or_else(|| vec![]))
    }

    pub fn get_all_instances(&self) -> Result<Vec<InstanceInfo>, DispatcherError> {
        let instances = self.get_instances()?;
        let mut all_instances = Vec::new();
        for instance_id in instances {
            if let Some(info) = self.get_instance(&instance_id)? {
                all_instances.push(info);
            }
        }
        Ok(all_instances)
    }

    pub fn save_instance(
        &self,
        instance_id: &str,
        status: &InstanceInfo,
    ) -> Result<(), DispatcherError> {
        self.db().set(&instance_key(instance_id), status, None)?;
        let mut instances = self.get_instances()?;
        if !instances.contains(&instance_id.to_string()) {
            instances.push(instance_id.to_string());
            self.db().set(&instances_key(), instances, None)?;
        }
        Ok(())
    }

    pub fn get_instance(&self, instance_id: &str) -> Result<Option<InstanceInfo>, DispatcherError> {
        Ok(self.db().get(&instance_key(instance_id))?)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use storage_backend::{storage::Storage, storage_config::StorageConfig};

    use super::*;
    use crate::helper::{get_storage_path, remove_storage_path};
    use test_helper::test_helper::init_trace;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct EchoMessage {
        pub content: String,
    }

    impl DispatcherMessage for EchoMessage {
        fn command(&self) -> Result<(String, Vec<String>, String), DispatcherError> {
            Ok((
                "sh".to_string(),
                vec![
                    "-c".to_string(),
                    format!("echo {} >output.json", self.content),
                ],
                "output.json".to_string(),
            ))
        }

        fn message_type(&self) -> String {
            "echo".to_string()
        }
    }

    fn init_storage() -> (Rc<DispatcherStorage>, String) {
        let storage_path = get_storage_path();
        let storage_config = StorageConfig::new(storage_path.to_string(), None);
        let storage = Rc::new(Storage::new(&storage_config).unwrap());
        let dispatcher_storage = Rc::new(DispatcherStorage::new(storage.clone()));

        (dispatcher_storage, storage_path)
    }

    fn init_dispatcher() -> (DispatcherAws, String) {
        let config_path = format!(
            "{}/../aws-service/config/config.yaml",
            env!("CARGO_MANIFEST_DIR")
        );
        let (dispatcher_storage, storage_path) = init_storage();

        (
            DispatcherAws::new(config_path, dispatcher_storage).unwrap(),
            storage_path,
        )
    }

    fn sample_job() -> DispatcherJob<EchoMessage> {
        DispatcherJob {
            job_id: "job1".to_string(),
            job_type: EchoMessage {
                content: "Hello, AWS!".to_string(),
            },
        }
    }

    fn sample_msg() -> Msg {
        let msg = sample_job();
        let encoded = serde_json::to_string(&msg).unwrap();
        Msg::new(encoded, "pubk:1".to_string().parse().unwrap())
    }

    #[test]
    fn test_dispatcher_aws() {
        init_trace();

        let (dispatcher, storage_path) = init_dispatcher();
        dispatcher.tick::<EchoMessage>().unwrap();
        drop(dispatcher);
        remove_storage_path(&storage_path);

        assert!(true);
    }

    #[test]
    fn test_dispatcher_aws_storage() {
        init_trace();
        let (storage, storage_path) = init_storage();
        let aws_storage = DispatcherAwsStorage::new(storage);

        // same instance is reflected in the list of instances
        let id = "1234";
        let status = InstanceInfo::new(id.to_string(), InstanceStatus::Running, "1".to_string());
        aws_storage.save_instance(id, &status).unwrap();
        let instances = aws_storage.get_instances().unwrap();
        assert_eq!(instances, vec![id.to_string()]);

        // save same instance and the list of instances should not have duplicates
        aws_storage.save_instance(id, &status).unwrap();
        let instances = aws_storage.get_instances().unwrap();
        assert_eq!(instances, vec![id.to_string()]);

        // get instance info
        let instance_info = aws_storage.get_instance(id).unwrap().unwrap();
        assert_eq!(instance_info.instance_id, id.to_string());

        // create another instance and check the list of instances
        let id2 = "5678";
        let status2 = InstanceInfo::new(id2.to_string(), InstanceStatus::Running, "2".to_string());
        aws_storage.save_instance(id2, &status2).unwrap();
        let instances = aws_storage.get_all_instances().unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].instance_id, id.to_string());
        assert_eq!(instances[1].instance_id, id2.to_string());

        drop(aws_storage);
        remove_storage_path(&storage_path);
    }

    #[test]
    fn test_job_assignment() {
        init_trace();

        let (dispatcher, storage_path) = init_dispatcher();
        dispatcher
            .storage
            .storage
            .persist_job("1", "dummy1")
            .unwrap();
        let assignment = dispatcher.assign_jobs().unwrap();
        assert_eq!(assignment, Some("1".to_string()));

        dispatcher
            .storage
            .save_instance(
                "instance-1",
                &InstanceInfo::new(
                    "instance-1".to_string(),
                    InstanceStatus::Init,
                    "1".to_string(),
                ),
            )
            .unwrap();

        let assignment = dispatcher.assign_jobs().unwrap();
        assert_eq!(assignment, None);

        drop(dispatcher);
        remove_storage_path(&storage_path);
    }

    #[test]
    #[ignore]
    fn test_job_execute() {
        init_trace();

        let (dispatcher, storage_path) = init_dispatcher();
        dispatcher
            .storage
            .storage
            .persist_job("1", &sample_msg().to_string())
            .unwrap();

        for _ in 0..30 {
            dispatcher.tick::<EchoMessage>().unwrap();
            std::thread::sleep(std::time::Duration::from_secs(10));
        }

        drop(dispatcher);
        remove_storage_path(&storage_path);
    }
}
