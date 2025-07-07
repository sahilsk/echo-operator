use crate::echo::{Echo, EchoSpec};
use anyhow::{Context, Result};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    Client,
    api::{Api, PostParams},
    runtime::{WatchStreamExt, watcher},
};
use kube::{CustomResourceExt, ResourceExt};
use serde::Serialize;
use std::thread::sleep;
use std::time::Duration;
use tracing::{Level, debug, info, warn};
use tracing_subscriber::FmtSubscriber;

pub mod echo;
use futures::TryStreamExt;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    let dry_run: bool = false;
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
    let jh = tokio::spawn(watch_echos(client.clone()));

    // Create echo instance
    let echo_spec = EchoSpec {
        message: "ls -al ".to_string(),
        count: 1,
    };

    create_echo(client.clone(), echo_spec.clone(), dry_run).await?;

    /********************
     * create watcher
     *******************/

    jh.await?;

    Ok(())
}

fn print_json<T>(rsrc: &T) -> Result<()>
where
    T: ?Sized + Serialize,
{
    let rsrc_json = serde_json::to_string_pretty(rsrc)?;
    debug!(r#"{}"#, &rsrc_json);

    Ok(())
}

fn print_yaml<T>(rsrc: &T) -> Result<()>
where
    T: ?Sized + Serialize,
{
    let rsrc_yaml = serde_yaml::to_string(rsrc)?;
    debug!(r#"{}"#, &rsrc_yaml);
    Ok(())
}

async fn apply_crd(client: Client, crd: CustomResourceDefinition, dry_run: bool) -> Result<()> {
    /*
     * -  `all`: cluster level resources eg. crd
     * - `default_namespaced`: namespaced resources eg. pod
     **/
    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let pp = PostParams {
        dry_run,
        field_manager: Default::default(),
    };
    let created_crd_result = crds.create(&pp, &crd).await;
    match created_crd_result {
        Ok(created_crd) => {
            let created_crd_yaml = serde_yaml::to_string(&created_crd).unwrap();
            info!("CRD Created: {}", &created_crd_yaml);
            // Wait for the api to catch up
            sleep(Duration::from_secs(1));
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

async fn watch_echos(client: Client) -> Result<()> {
    let echos: Api<Echo> = Api::namespaced(client, "default");
    let echo_filter: watcher::Config = watcher::Config::default().fields("apiVersion=sonukr.rs/v1");

    watcher(echos, echo_filter)
        .touched_objects()
        .try_for_each(|p| async move { process_echo(p).await })
        .await?;

    Ok(())
}

async fn process_echo(p: Echo) -> Result<(), watcher::Error> {
    println!("Applied: {}", p.name_any());
    Ok(())
}

/// Create instance of 'Echo' resource
async fn create_echo(client: Client, echo_spec: EchoSpec, dry_run: bool) -> Result<()> {
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
