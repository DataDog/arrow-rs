// Licensed to the Apache Software Foundation (ASF) under one
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

use crate::client::pagination::stream_paginated;
use crate::path::Path;
use crate::Result;
use crate::{ListResult, ObjectMeta};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeSet;

/// A client that can perform paginated list requests
#[async_trait]
pub trait ListClient: Send + Sync + 'static {
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)>;

    /// A list request that includes object versions, for stores that support versioning
    async fn list_versions_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        key_token: Option<&str>,
        _version_token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>, Option<String>)> {
        // Default implementation just forwards to list_request
        // This method should be overridden by stores that support versioning
        match self
            .list_request(prefix, delimiter, key_token, offset)
            .await
        {
            Ok((list_result, next_token)) => Ok((list_result, next_token, None)),
            Err(e) => Err(e),
        }
    }
}

/// Extension trait for [`ListClient`] that adds common listing functionality
#[async_trait]
pub trait ListClientExt {
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
        offset: Option<&Path>,
    ) -> BoxStream<'_, Result<ListResult>>;

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>>;

    fn list_versions(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>>;

    #[allow(unused)]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>>;

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult>;
}

#[async_trait]
impl<T: ListClient> ListClientExt for T {
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
        offset: Option<&Path>,
    ) -> BoxStream<'_, Result<ListResult>> {
        let offset = offset.map(|x| x.to_string());
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER));

        stream_paginated(
            (prefix, offset),
            move |(prefix, offset), token| async move {
                let (r, next_token) = self
                    .list_request(
                        prefix.as_deref(),
                        delimiter,
                        token.as_deref(),
                        offset.as_deref(),
                    )
                    .await?;
                Ok((r, (prefix, offset), next_token))
            },
        )
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.list_paginated(prefix, false, None)
            .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
            .try_flatten()
            .boxed()
    }

    fn list_versions(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let prefix = prefix
            .filter(|x| !x.as_ref().is_empty())
            .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER));

        stream_paginated(
            prefix,
            move |prefix, tokens: Option<(Option<String>, Option<String>)>| async move {
                let token = tokens.as_ref().and_then(|t| t.0.as_deref());
                let version_token = tokens.as_ref().and_then(|t| t.1.as_deref());
                let (r, next_token, next_version) = self
                    .list_versions_request(prefix.as_deref(), false, token, version_token, None)
                    .await?;
                let next_tokens = match (next_token, next_version) {
                    (Some(t), Some(v)) => Some((Some(t), Some(v))),
                    (Some(t), None) => Some((Some(t), None)),
                    (None, Some(v)) => Some((None, Some(v))),
                    (None, None) => None,
                };
                Ok((r, prefix, next_tokens))
            },
        )
        .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        self.list_paginated(prefix, false, Some(offset))
            .map_ok(|r| futures::stream::iter(r.objects.into_iter().map(Ok)))
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut stream = self.list_paginated(prefix, true, None);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(result) = stream.next().await {
            let response = result?;
            common_prefixes.extend(response.common_prefixes.into_iter());
            objects.extend(response.objects.into_iter());
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
}
