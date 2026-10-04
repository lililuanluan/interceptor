// https://crates.io/crates/clap-config-file
use chrono::Local;
use clap_config_file::ClapConfigFile;
use std::path::PathBuf;

use anyhow::{Result, ensure};

use crate::filepath::create_shared_dir;

#[derive(ClapConfigFile)]
#[config_file_name = "interceptor"] // 自动发现interceptor.yaml/json等，TODO：禁用这个，必须显式提供！
#[config_file_formats = "yaml,json,toml"]
pub struct Config {
    #[config_arg(default_value = "4")] // 3f+1
    pub num_nodes: u32,

    #[config_arg(default_value = "cometbft/cometbft:v0.38.21")]
    pub docker_image: String,

    #[config_arg(accept_from = "config_only")] // 只从配置文件读取，这个库有问题
    pub auto_pull_image: Option<bool>, // TODO: 将option<bool>换成别的

    #[config_arg(accept_from = "cli_and_config")]
    pub cluster_id: Option<String>,

    #[config_arg(default_value = "10")]
    pub max_block: u32, // run 0-max_block blocks
}

#[derive(Debug)]
pub struct RunPaths {
    pub testnet_config_dir: PathBuf,
    pub test_log_dir: PathBuf,
}

impl RunPaths {
    pub fn new(config_dir: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            testnet_config_dir: config_dir,
            test_log_dir: log_dir,
        }
    }
}

impl Config {
    // TODO: 外部手动指定这两个路径
    pub fn get_run_paths(&self) -> Result<RunPaths> {
        let cluster_id = self
            .cluster_id
            .clone()
            .unwrap_or_else(|| Local::now().format("%Y-%m-%d-%H-%M-%S").to_string());

        ensure!(
            !cluster_id.is_empty()
                && cluster_id
                    .bytes()
                    .all(|b| { b.is_ascii_alphanumeric() || b == b'-' || b == b'_' }),
            "cluster_id must contain only alphabets, numbers, - and _!"
        );

        let root = PathBuf::from("/tmp");

        // 如果root不存在，不要创建！直接退出
        ensure!(root.exists(), "root {:?} doesn't exist", root);

        let run_paths = RunPaths {
            testnet_config_dir: root.clone().join(&cluster_id).join("config"),
            test_log_dir: root.clone().join(&cluster_id).join("logs"),
        };
        // 创建一下 root/cluster_id
        create_shared_dir(&root.join(&cluster_id))?;
        Ok(run_paths)
    }
}
