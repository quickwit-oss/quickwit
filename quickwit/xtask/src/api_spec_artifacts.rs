use std::collections::HashMap;
use std::{fs, io};

use anyhow::Context;
use chrono::{DateTime, FixedOffset};
use reqwest::blocking as reqwest;
use serde::{Deserialize, Serialize};

use super::*;

pub fn download_artifacts() -> anyhow::Result<()> {
    let api_spec_dir = ROOT_DIR
        .join(ES_ARTIFACTS_DIR_NAME)
        .join(&*ES_STACK_VERSION);
    let artifacts_url = format!(
        "https://artifacts-api.elastic.co/v1/versions/{}",
        *ES_STACK_VERSION
    );
    println!("Downloading build info from {}", &artifacts_url);

    let artifacts = reqwest::get(&artifacts_url)?.json::<Artifacts>()?;
    let project = artifacts
        .version
        .builds
        .iter()
        .max_by_key(|build| build.start_time)
        .unwrap()
        .projects
        .get("elasticsearch")
        .with_context(|| "Project 'elasticsearch' not found")?;

    if let Some(local_project) = read_local_project()? {
        if local_project.commit_hash == project.commit_hash {
            println!("Specs already downloaded.");
            return Ok(());
        }
    }

    let specs_url = project
        .packages
        .get(&format!("rest-resources-zip-{}.zip", *ES_STACK_VERSION))
        .with_context(|| {
            format!(
                "Package `rest-resources-zip-{}.zip` not found",
                *ES_STACK_VERSION
            )
        })?
        .url
        .clone();

    println!("Downloading specs and yaml tests from {}", &specs_url);
    let zip_resp = reqwest::get(&specs_url)?.bytes()?;

    if api_spec_dir.exists() {
        fs::remove_dir_all(&api_spec_dir).unwrap();
    }
    fs::create_dir_all(&api_spec_dir).unwrap();
    zip::ZipArchive::new(io::Cursor::new(zip_resp))?.extract(&api_spec_dir)?;

    // Write project metadata for reference
    let project_path = &api_spec_dir.join("elasticsearch.json");
    let project_file = fs::File::create(&project_path)?;
    serde_json::to_writer_pretty(project_file, &project)?;

    Ok(())
}

fn read_local_project() -> anyhow::Result<Option<Project>> {
    let spec_dir = ROOT_DIR
        .join(ES_ARTIFACTS_DIR_NAME)
        .join(&*ES_STACK_VERSION);
    let project_path = &spec_dir.join("elasticsearch.json");

    if project_path.exists() {
        Ok(serde_json::from_reader(fs::File::open(project_path)?)?)
    } else {
        Ok(None)
    }
}

#[derive(Deserialize, Debug)]
pub struct Artifacts {
    pub version: Version,
    // manifests
}

#[derive(Deserialize, Debug)]
pub struct Version {
    pub version: String,
    pub builds: Vec<Build>,
}

#[derive(Deserialize, Debug)]
pub struct Build {
    pub projects: HashMap<String, Project>,
    #[serde(with = "rfc2822_format")]
    pub start_time: DateTime<FixedOffset>,
    pub release_branch: String,
    pub version: String,
    pub branch: String,
    pub build_id: String,
    // end_time: String,
    // prefix: String,
    // manifest_version: String,
    // build_duration_seconds: u32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Project {
    pub branch: String,
    pub commit_hash: String,
    pub packages: HashMap<String, Package>,
    // commit_url
    // build_duration_seconds: u32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Package {
    pub url: String,
    pub sha_url: String,
    pub asc_url: String,
    pub architecture: Option<String>,

    #[serde(rename = "type")]
    pub type_: String,
    pub classifier: Option<String>,
    pub attributes: Option<Attributes>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Attributes {
    pub internal: Option<String>,
    pub artifact_id: Option<String>,
    pub oss: Option<String>,
    pub group: Option<String>,
}

#[allow(dead_code)]
mod rfc2822_format {
    use chrono::{DateTime, FixedOffset};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &DateTime<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let s = date.to_rfc2822();
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc2822(&s).map_err(serde::de::Error::custom)
    }
}
