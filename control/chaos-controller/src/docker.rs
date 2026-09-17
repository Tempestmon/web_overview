use std::time::Duration;

use bollard::Docker;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerStateStatusEnum, HealthStatusEnum};
use bollard::query_parameters::StopContainerOptions;
use futures::StreamExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerStatus {
    /// Running and either healthy or without a health check.
    Serving,
    /// Running, health check not yet green.
    Starting,
    /// Running, health check red.
    Unhealthy,
    Paused,
    Exited,
    Missing,
}

/// The tiny slice of the Docker API the controller is allowed to use.
/// `docker-socket-proxy` enforces the same allowlist on the wire.
#[async_trait::async_trait]
pub trait ContainerControl: Send + Sync {
    async fn status(&self, name: &str) -> anyhow::Result<ContainerStatus>;
    async fn stop(&self, name: &str) -> anyhow::Result<()>;
    async fn start(&self, name: &str) -> anyhow::Result<()>;
    async fn pause(&self, name: &str) -> anyhow::Result<()>;
    async fn unpause(&self, name: &str) -> anyhow::Result<()>;
    /// Runs a command inside the container as root and fails on a non-zero
    /// exit, returning the command's output as the error text.
    async fn exec(&self, name: &str, cmd: &[&str]) -> anyhow::Result<()>;
}

pub struct BollardControl {
    docker: Docker,
}

impl BollardControl {
    pub fn connect(url: &str) -> anyhow::Result<Self> {
        let docker = Docker::connect_with_http(url, 10, bollard::API_DEFAULT_VERSION)?;
        Ok(Self { docker })
    }
}

#[async_trait::async_trait]
impl ContainerControl for BollardControl {
    async fn status(&self, name: &str) -> anyhow::Result<ContainerStatus> {
        let inspect = match self.docker.inspect_container(name, None).await {
            Ok(i) => i,
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                return Ok(ContainerStatus::Missing);
            }
            Err(err) => return Err(err.into()),
        };
        let state = inspect.state.unwrap_or_default();
        let health = state.health.and_then(|h| h.status);
        Ok(match state.status {
            Some(ContainerStateStatusEnum::RUNNING) => match health {
                Some(HealthStatusEnum::STARTING) => ContainerStatus::Starting,
                Some(HealthStatusEnum::UNHEALTHY) => ContainerStatus::Unhealthy,
                _ => ContainerStatus::Serving,
            },
            Some(ContainerStateStatusEnum::PAUSED) => ContainerStatus::Paused,
            Some(ContainerStateStatusEnum::RESTARTING) => ContainerStatus::Starting,
            _ => ContainerStatus::Exited,
        })
    }

    async fn stop(&self, name: &str) -> anyhow::Result<()> {
        let opts = StopContainerOptions {
            signal: None,
            t: Some(5),
        };
        self.docker.stop_container(name, Some(opts)).await?;
        Ok(())
    }

    async fn start(&self, name: &str) -> anyhow::Result<()> {
        self.docker.start_container(name, None).await?;
        Ok(())
    }

    async fn pause(&self, name: &str) -> anyhow::Result<()> {
        self.docker.pause_container(name).await?;
        Ok(())
    }

    async fn unpause(&self, name: &str) -> anyhow::Result<()> {
        self.docker.unpause_container(name).await?;
        Ok(())
    }

    async fn exec(&self, name: &str, cmd: &[&str]) -> anyhow::Result<()> {
        let options = CreateExecOptions {
            cmd: Some(cmd.iter().map(|s| s.to_string()).collect()),
            // Containers run as an unprivileged user; tc needs the container's
            // NET_ADMIN, which only root inside it can exercise.
            user: Some("root".to_string()),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };
        let created = self.docker.create_exec(name, options).await?;
        let mut output = String::new();
        if let StartExecResults::Attached {
            output: mut stream, ..
        } = self.docker.start_exec(&created.id, None).await?
        {
            while let Some(chunk) = stream.next().await {
                output.push_str(&chunk?.to_string());
            }
        }
        let inspected = self.docker.inspect_exec(&created.id).await?;
        match inspected.exit_code {
            Some(0) | None => Ok(()),
            Some(code) => {
                anyhow::bail!("`{}` exited with {code}: {}", cmd.join(" "), output.trim())
            }
        }
    }
}

pub const STATUS_POLL: Duration = Duration::from_secs(2);
