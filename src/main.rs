use crate::echo::{Echo, EchoSpec};
use anyhow::Result;
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::{
    api::core::v1::{Container, PodSpec},
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
};
use kube::runtime::watcher::Error;
use kube::{
    Client,
    api::{Api, PostParams},
    runtime::{WatchStreamExt, watcher},
};
use kube::{CustomResourceExt, ResourceExt};
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinError;
use tracing::{Level, debug, info, warn};
use tracing_subscriber::FmtSubscriber;

pub mod echo;
use futures::TryStreamExt;

static CLIENT_NAME: &str = "kube.rs";

#[derive(Error, Debug)]
pub enum EchoOperatorError {
    #[error("Error watching for events {0}")]
    KubeWatcherError(#[from] Error),

    #[error("Error kube api {0}")]
    TokioJoinError(#[from] JoinError),

    #[error("Error kube error {0}")]
    KubeError(#[from] kube::Error),

    #[error("Anyhow Error {0}")]
    AnyhowError(#[from] anyhow::Error),

    #[error("Yaml Parse Error {0}")]
    ParseYamlError(#[from] serde_yaml::Error),

    #[error("Json Parse Error {0}")]
    ParseJsonError(#[from] serde_json::Error),

    #[error("processing error : {0}")]
    ProcesingError(String),
}

#[tokio::main]
async fn main() -> Result<(), EchoOperatorError> {
    let dry_run: bool = true;
    let client = Client::try_default().await?;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    /**********************
    // Create new CRD: Echo
     **********************/
    let d = Echo::new("guide", EchoSpec::default());
    info!("EchoSpec: {:#?}", d);

    let echo_crd = Echo::crd();
    print_yaml(&echo_crd)?;
    print_json(&echo_crd)?;

    /********************
    // Apply CRD to k8s
     *******************/
    apply_crd(client.clone(), echo_crd.clone(), dry_run).await?;

    // Create watcher
    let jh = tokio::spawn(watch_echos(client.clone(), dry_run));

    // Create echo instance
    let echo_spec = EchoSpec {
        message: "ls -al ".to_string(),
        count: 1,
    };

    create_echo(client.clone(), echo_spec.clone(), dry_run).await?;

    /********************
     * create watcher
     *******************/

    jh.await?
}

fn print_json<T>(rsrc: &T) -> Result<(), EchoOperatorError>
where
    T: ?Sized + Serialize,
{
    let rsrc_json = serde_json::to_string_pretty(rsrc)?;
    debug!(r#"{}"#, &rsrc_json);

    Ok(())
}

fn print_yaml<T>(rsrc: &T) -> Result<(), EchoOperatorError>
where
    T: ?Sized + Serialize,
{
    let rsrc_yaml = serde_yaml::to_string(rsrc)?;
    debug!(r#"{}"#, &rsrc_yaml);
    Ok(())
}

async fn apply_crd(
    client: Client,
    crd: CustomResourceDefinition,
    dry_run: bool,
) -> Result<(), EchoOperatorError> {
    /*
     * -  `all`: cluster level resources eg. crd
     * - `default_namespaced`: namespaced resources eg. pod
     **/
    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let pp = PostParams {
        dry_run,
        field_manager: Some(CLIENT_NAME.into()),
    };
    let created_crd_result = crds.create(&pp, &crd).await;
    match created_crd_result {
        Ok(created_crd) => {
            let created_crd_yaml = serde_yaml::to_string(&created_crd).unwrap();
            info!("CRD Created: {}", &created_crd_yaml);
            // Wait for the api to catch up
            tokio::time::sleep(Duration::from_secs(1));
        }
        Err(e) => match e {
            kube::Error::Api(er) => {
                if er.to_string().contains("AlreadyExists") {
                    warn!("CRD: {:#?} already exists", &crd.metadata.name.unwrap());
                } else {
                    warn!("*ApiError occurred: {}", er);
                }
            }
            _ => {
                warn!("Unknown Error occurred: {}", &e);
            }
        },
    };

    Ok(())
}

async fn watch_echos(client: Client, dry_run: bool) -> Result<(), EchoOperatorError> {
    let echos: Api<Echo> = Api::namespaced(client.clone(), "default");
    let echo_filter: watcher::Config = watcher::Config::default();

    watcher(echos, echo_filter)
        .touched_objects()
        .try_for_each(|p| async { process_echo(p, client.clone(), dry_run).await })
        .await
        .map_err(EchoOperatorError::KubeWatcherError)?;

    Ok(())
}

// async fn process_echo(p: Echo) -> Result<(), watcher::Error> {
async fn process_echo(p: Echo, client: Client, dry_run: bool) -> Result<(), EchoOperatorError> {
    info!("Name: {}", std::any::type_name_of_val(&p));
    info!(
        "Name: {}, Message: {},| Count: {}",
        p.name_any(),
        p.spec.message,
        p.spec.count
    );

    create_job(client, dry_run, "default", "curl google.com".to_string()).await?;
    Ok(())
}

async fn create_echo(
    client: Client,
    echo_spec: EchoSpec,
    dry_run: bool,
) -> Result<(), EchoOperatorError> {
    // let echos: Api<Echo> = Api::default_namespaced(client);
    let echos: Api<Echo> = Api::namespaced(client, "default");
    let echo_inst = Echo::new("echo01", echo_spec);

    let pp = PostParams {
        dry_run,
        ..Default::default()
    };

    info!("{}", serde_yaml::to_string(&echo_inst).unwrap());
    match echos.create(&pp, &echo_inst).await {
        Ok(echo) => {
            info!(name: "Resource:Echo", "Echo instance created: {:#?}", echo.metadata.name.unwrap());
        }
        Err(e) => match e {
            kube::Error::Api(er) => {
                if er.reason == "AlreadyExists" {
                    warn!( name: "Resource:Echo", "Resource already exist:  '{}'", echo_inst.metadata.name.unwrap());
                } else {
                    warn!( name: "Resource:Echo",  "Error occurred: {:#?}", er);
                }
            }
            _ => warn!( name: "Resource:Echo",  "Error occurred: {:#?}", e),
        },
    }

    Ok(())
}

async fn create_job(
    client: Client,
    dry_run: bool,
    namespace: &str,
    command: String,
) -> Result<Job, EchoOperatorError> {
    let jobs: Api<Job> = Api::namespaced(client, namespace);
    let pp = PostParams {
        dry_run,
        field_manager: Some(CLIENT_NAME.into()),
    };

    let job_template = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            name: Some("new_pod".to_string()),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![Container {
                image: Some("curlimages/curl".to_string()),
                command: Some(vec![command]),
                ..Default::default()
            }],
            ..Default::default()
        }),
    };

    let data = Job {
        metadata: ObjectMeta {
            name: Some("new_job".to_string()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            template: job_template,
            ..Default::default()
        }),
        status: None,
    };

    jobs.create(&pp, &data)
        .await
        .map_err(EchoOperatorError::KubeError)
}
