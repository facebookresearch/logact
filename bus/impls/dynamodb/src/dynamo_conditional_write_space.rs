/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use agentbus_api::ConditionalWriteError;
use agentbus_api::ConditionalWriteResult;
use agentbus_api::ConditionalWriteSpace;
use agentbus_api::TailError;
use agentbus_api::TailResult;
use agentbus_api::TailableSpace;
use agentbus_api::Version;
use agentbus_api::VersionedValue;
use anyhow::Context;
use anyhow::Error as AnyhowError;
use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::KeySchemaElement;
use aws_sdk_dynamodb::types::KeyType;
use bytes::Bytes;

const SPACE_ID_COLUMN: &str = "space_id";
const ADDRESS_COLUMN: &str = "address";
const VAL_COLUMN: &str = "val";
const VER_COLUMN: &str = "ver";

/// A ConditionalWriteSpace backed by DynamoDB.
#[derive(Clone)]
pub struct DynamoConditionalWriteSpace {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

impl DynamoConditionalWriteSpace {
    pub async fn new(client: aws_sdk_dynamodb::Client, table_name: String) -> Result<Self> {
        let output = client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .context("failed to describe table")?;

        let table = output.table.context("missing table description")?;
        let key_schema = table.key_schema();

        let has_hash = key_schema.iter().any(|k: &KeySchemaElement| {
            k.attribute_name() == SPACE_ID_COLUMN && k.key_type() == &KeyType::Hash
        });
        let has_range = key_schema.iter().any(|k: &KeySchemaElement| {
            k.attribute_name() == ADDRESS_COLUMN && k.key_type() == &KeyType::Range
        });
        anyhow::ensure!(
            has_hash && has_range,
            "table '{}' must have partition key '{}' (S, HASH) and sort key '{}' (N, RANGE)",
            table_name,
            SPACE_ID_COLUMN,
            ADDRESS_COLUMN,
        );

        Ok(Self { client, table_name })
    }
}

fn version_from_u64(n: u64) -> Version {
    Version(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn try_version_to_u64(v: &Version) -> Option<u64> {
    let bytes: [u8; 8] = v.0[..].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

impl TailableSpace for DynamoConditionalWriteSpace {
    async fn tail(&self, space_id: &str, window_size: u64) -> TailResult<u64> {
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression(format!("{} = :sid", SPACE_ID_COLUMN))
            .expression_attribute_values(":sid", AttributeValue::S(space_id.to_string()))
            .projection_expression(ADDRESS_COLUMN)
            .scan_index_forward(false)
            .consistent_read(true)
            .limit(window_size as i32)
            .send()
            .await
            .map_err(|e| TailError::BackendUnavailable(e.into_service_error().to_string()))?;

        let written: std::collections::HashSet<u64> = result
            .items()
            .iter()
            .filter_map(|item| {
                item.get(ADDRESS_COLUMN).and_then(|v| match v {
                    AttributeValue::N(n) => n.parse().ok(),
                    _ => None,
                })
            })
            .collect();

        let Some(&max_addr) = written.iter().max() else {
            return Ok(0);
        };
        let end = max_addr + 1;

        let window_start = end.saturating_sub(window_size);
        Ok((window_start..end)
            .find(|addr| !written.contains(addr))
            .unwrap_or(end))
    }
}

impl ConditionalWriteSpace for DynamoConditionalWriteSpace {
    async fn write(
        &mut self,
        space_id: &str,
        address: u64,
        expected_version: Option<Version>,
        value: Bytes,
    ) -> ConditionalWriteResult<bool> {
        let (new_version, condition_expr, condition_val) = match expected_version {
            None => (0u64, "attribute_not_exists(#ver)", None),
            Some(v) => {
                let current = match try_version_to_u64(&v) {
                    Some(n) => n,
                    None => return Ok(false),
                };
                (
                    current + 1,
                    "#ver = :expected_ver",
                    Some(current.to_string()),
                )
            }
        };

        // TODO: Handle values exceeding the DynamoDB 400KB item size limit.
        let mut req = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item(SPACE_ID_COLUMN, AttributeValue::S(space_id.to_string()))
            .item(ADDRESS_COLUMN, AttributeValue::N(address.to_string()))
            .item(VER_COLUMN, AttributeValue::N(new_version.to_string()))
            .item(
                VAL_COLUMN,
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(value)),
            )
            .condition_expression(condition_expr)
            .expression_attribute_names("#ver", VER_COLUMN);

        if let Some(ver_str) = condition_val {
            req = req.expression_attribute_values(":expected_ver", AttributeValue::N(ver_str));
        }

        match req.send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                let service_err = err.into_service_error();
                if service_err.is_conditional_check_failed_exception() {
                    Ok(false)
                } else {
                    Err(ConditionalWriteError::BackendUnavailable(AnyhowError::new(
                        service_err,
                    )))
                }
            }
        }
    }

    async fn read(
        &self,
        space_id: &str,
        address: u64,
    ) -> ConditionalWriteResult<Option<VersionedValue>> {
        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(SPACE_ID_COLUMN, AttributeValue::S(space_id.to_string()))
            .key(ADDRESS_COLUMN, AttributeValue::N(address.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| {
                ConditionalWriteError::BackendUnavailable(AnyhowError::new(e.into_service_error()))
            })?;

        let Some(item) = result.item() else {
            return Ok(None);
        };

        let version = match item.get(VER_COLUMN) {
            Some(AttributeValue::N(n)) => {
                let v = n
                    .parse::<u64>()
                    .map_err(|e| ConditionalWriteError::BackendUnavailable(AnyhowError::new(e)))?;
                version_from_u64(v)
            }
            _ => return Ok(None),
        };

        let value = match item.get(VAL_COLUMN) {
            Some(AttributeValue::B(blob)) => Bytes::copy_from_slice(blob.as_ref()),
            _ => return Ok(None),
        };

        Ok(Some(VersionedValue { version, value }))
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput;
    use aws_sdk_dynamodb::types::KeySchemaElement;
    use aws_sdk_dynamodb::types::KeyType;
    use aws_sdk_dynamodb::types::TableDescription;
    use aws_smithy_mocks::MockResponseInterceptor;
    use aws_smithy_mocks::Rule;
    use aws_smithy_mocks::RuleMode;
    use aws_smithy_mocks::create_mock_http_client;
    use aws_smithy_mocks::mock;

    use super::*;

    fn mock_client(rules: &[&Rule]) -> aws_sdk_dynamodb::Client {
        let mut interceptor = MockResponseInterceptor::new().rule_mode(RuleMode::Sequential);
        for rule in rules {
            interceptor = interceptor.with_rule(rule);
        }
        aws_sdk_dynamodb::Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .region(aws_sdk_dynamodb::config::Region::from_static("us-east-1"))
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .http_client(create_mock_http_client())
                .interceptor(interceptor)
                .behavior_version_latest()
                .build(),
        )
    }

    fn valid_table_description() -> TableDescription {
        TableDescription::builder()
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(SPACE_ID_COLUMN)
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(ADDRESS_COLUMN)
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[tokio::test]
    async fn test_new_accepts_valid_schema() {
        let rule = mock!(aws_sdk_dynamodb::Client::describe_table).then_output(|| {
            DescribeTableOutput::builder()
                .table(valid_table_description())
                .build()
        });
        let client = mock_client(&[&rule]);

        DynamoConditionalWriteSpace::new(client, "test-table".to_string())
            .await
            .expect("valid schema should succeed");
    }

    #[tokio::test]
    async fn test_new_rejects_missing_sort_key() {
        let rule = mock!(aws_sdk_dynamodb::Client::describe_table).then_output(|| {
            DescribeTableOutput::builder()
                .table(
                    TableDescription::builder()
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name(SPACE_ID_COLUMN)
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
        });
        let client = mock_client(&[&rule]);

        let err = DynamoConditionalWriteSpace::new(client, "test-table".to_string())
            .await
            .err()
            .expect("should fail with missing sort key");
        assert!(err.to_string().contains("must have partition key"));
    }

    #[tokio::test]
    async fn test_new_rejects_wrong_partition_key_name() {
        let rule = mock!(aws_sdk_dynamodb::Client::describe_table).then_output(|| {
            DescribeTableOutput::builder()
                .table(
                    TableDescription::builder()
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("wrong_name")
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name(ADDRESS_COLUMN)
                                .key_type(KeyType::Range)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
        });
        let client = mock_client(&[&rule]);

        let result = DynamoConditionalWriteSpace::new(client, "test-table".to_string()).await;
        assert!(result.is_err(), "should fail with wrong partition key name");
    }

    #[tokio::test]
    async fn test_new_rejects_nonexistent_table() {
        let rule = mock!(aws_sdk_dynamodb::Client::describe_table).then_error(|| {
            aws_sdk_dynamodb::operation::describe_table::DescribeTableError::ResourceNotFoundException(
                aws_sdk_dynamodb::types::error::ResourceNotFoundException::builder()
                    .message("Table not found")
                    .build(),
            )
        });
        let client = mock_client(&[&rule]);

        let err = DynamoConditionalWriteSpace::new(client, "test-table".to_string())
            .await
            .err()
            .expect("should fail with nonexistent table");
        assert!(err.to_string().contains("failed to describe table"));
    }
}
