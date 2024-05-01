// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The list and multipart API used by both GCS and S3

use crate::multipart::PartId;
use crate::path::Path;
use crate::{Error, ListResult, ObjectMeta, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

impl TryFrom<ListResponse> for ListResult {
    type Error = crate::Error;

    fn try_from(value: ListResponse) -> Result<Self> {
        let common_prefixes = value
            .common_prefixes
            .into_iter()
            .map(|x| Ok(Path::parse(x.prefix)?))
            .collect::<Result<_>>()?;

        let objects = value
            .contents
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_>>()?;

        Ok(Self {
            common_prefixes,
            objects,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

impl TryFrom<ListContents> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: ListContents) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.key)?,
            last_modified: value.last_modified,
            size: value.size,
            e_tag: value.e_tag,
            version: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUpload {
    pub part: Vec<MultipartPart>,
}

impl From<Vec<PartId>> for CompleteMultipartUpload {
    fn from(value: Vec<PartId>) -> Self {
        let part = value
            .into_iter()
            .enumerate()
            .map(|(part_number, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_number + 1,
            })
            .collect();
        Self { part }
    }
}

#[derive(Debug, Serialize)]
pub struct MultipartPart {
    #[serde(rename = "ETag")]
    pub e_tag: String,
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Tagging {
    #[serde(rename = "TagSet")]
    pub list: TagList,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagList {
    #[serde(rename = "Tag", default)]
    pub tags: Vec<Tag>,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl From<HashMap<String, String>> for Tagging {
    fn from(value: HashMap<String, String>) -> Self {
        let tags = value
            .into_iter()
            .map(|(key, value)| Tag { key, value })
            .collect();
        Self {
            list: TagList { tags },
        }
    }
}

impl Into<HashMap<String, String>> for Tagging {
    fn into(self) -> HashMap<String, String> {
        self.list
            .tags
            .into_iter()
            .map(|tag| (tag.key, tag.value))
            .collect()
    }
}

impl Tagging {
    pub fn to_xml_document(&self) -> Result<String> {
        let body = quick_xml::se::to_string(self).map_err(|e| Error::Generic {
            store: "",
            source: Box::new(e),
        })?;
        Ok(format!(r#"<?xml version="1.0" encoding="utf-8"?>{}"#, body))
    }

    pub fn to_xml_document_for_azure(&self) -> Result<String> {
        let body =
            quick_xml::se::to_string_with_root("Tags", self).map_err(|e| Error::Generic {
                store: "",
                source: Box::new(e),
            })?;
        Ok(format!(r#"<?xml version="1.0" encoding="utf-8"?>{}"#, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tagging() {
        let expected_xml = r#"<?xml version="1.0" encoding="utf-8"?><Tagging><TagSet><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></TagSet></Tagging>"#;

        let tags = Tagging {
            list: TagList {
                tags: vec![
                    Tag {
                        key: "key1".to_string(),
                        value: "value1".to_string(),
                    },
                    Tag {
                        key: "key2".to_string(),
                        value: "value2".to_string(),
                    },
                ],
            },
        };
        let body = tags.to_xml_document().unwrap();
        assert_eq!(body, expected_xml);
    }

    #[test]
    fn test_tagging_azure() {
        let expected_xml = r#"<?xml version="1.0" encoding="utf-8"?><Tags><TagSet><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></TagSet></Tags>"#;

        let tags = Tagging {
            list: TagList {
                tags: vec![
                    Tag {
                        key: "key1".to_string(),
                        value: "value1".to_string(),
                    },
                    Tag {
                        key: "key2".to_string(),
                        value: "value2".to_string(),
                    },
                ],
            },
        };
        let body = tags.to_xml_document_for_azure().unwrap();
        assert_eq!(body, expected_xml);
    }
}
