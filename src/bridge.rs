//  streamqueue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/streamqueue

use crate::config::Config;
use crate::route_runner::{RouteRunner, RouteRunnerCommand};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info, warn};

pub struct Bridge {
    runners: Arc<Mutex<HashMap<String, mpsc::Sender<RouteRunnerCommand>>>>,
    config: Config,
    route_tasks: Arc<Mutex<JoinSet<()>>>,
    shutdown_rx: watch::Receiver<()>,
    shutdown_tx: watch::Sender<()>,
}

impl Bridge {
    /// Creates a new Bridge from a configuration object.
    pub fn new(config: Config) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        Self {
            runners: Arc::new(Mutex::new(HashMap::new())),
            config,
            route_tasks: Arc::new(Mutex::new(JoinSet::new())),
            shutdown_rx,
            shutdown_tx,
        }
    }

    /// Returns a `watch::Sender` that can be used to trigger a graceful shutdown of the bridge.
    pub fn get_shutdown_handle(&self) -> watch::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Triggers a flush on all route publishers.
    pub async fn flush_routes(&self) {
        let runners = self.runners.lock().await;
        for (name, runner_tx) in runners.iter() {
            if runner_tx.is_closed() {
                continue;
            }
            if let Err(e) = runner_tx.try_send(RouteRunnerCommand::Flush) {
                warn!(route = %name, "Failed to send flush command to route runner: {}", e);
            }
        }
    }

    /// Runs all configured routes and returns a `JoinHandle` for the main bridge task.
    /// The bridge will run until all routes have completed (e.g., file EOF) or a shutdown
    /// signal is received.
    pub fn run(&mut self) -> JoinHandle<Result<()>> {
        let mut shutdown_rx = self.shutdown_rx.clone();
        let routes = self.config.routes.clone();
        let runners = self.runners.clone();
        let route_tasks_handle = self.route_tasks.clone();
        let global_config = self.config.clone();
        tokio::spawn(async move {
            info!("Bridge starting up...");
            let mut route_tasks = route_tasks_handle.lock().await;
            let num_routes = routes.len();
            let barrier = Arc::new(tokio::sync::Barrier::new(num_routes));

            for (name, route) in routes {
                let (runner_tx, runner_rx) = mpsc::channel(1);
                runners.lock().await.insert(name.clone(), runner_tx);
                let route_runner = RouteRunner::new(
                    name,
                    route,
                    global_config.clone(),
                    barrier.clone(),
                    shutdown_rx.clone(),
                    runner_rx,
                );
                route_tasks.spawn(route_runner.run());
            }

            if route_tasks.is_empty() {
                warn!("No routes configured or initialized. Bridge will shut down.");
                return Ok(());
            }

            // If there are routes, wait for them to complete or for a shutdown signal.
            // If there are no routes, just wait for the shutdown signal.
            if !route_tasks.is_empty() {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Global shutdown signal received. Draining all routes.");
                    }
                    _ = async {
                        while let Some(res) = route_tasks.join_next().await {
                            if let Err(e) = res {
                                error!("A route task panicked or failed: {}", e);
                            }
                        }
                    } => {
                        info!("All routes have completed their work. Bridge shutting down.");
                    }
                }
            } else {
                // No routes, so just wait for the shutdown signal.
                let _ = shutdown_rx.changed().await;
                info!("Global shutdown signal received. Draining all routes.");
            }

            // Ensure all tasks are finished.
            route_tasks.shutdown().await;
            info!("Bridge has shut down.");
            Ok(())
        })
    }

    /// Waits for all route tasks to complete.
    pub async fn wait_for_completion(&self) {
        let mut tasks = self.route_tasks.lock().await;
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A route task panicked or failed during wait_for_completion: {}", e);
            }
        }
    }
}
