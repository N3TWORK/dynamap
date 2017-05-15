/*
    Copyright 2017 N3TWORK INC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DynaMap {

    private static final Logger logger = LoggerFactory.getLogger(DynaMap.class);

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;
    private final SchemaRegistry schemaRegistry;
    private final String tablePrefix;
    private final ObjectMapper objectMapper;

    public DynaMap(AmazonDynamoDB amazonDynamoDB, String tablePrefix, SchemaRegistry schemaRegistry, ObjectMapper objectMapper) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
        this.schemaRegistry = schemaRegistry;
        this.tablePrefix = tablePrefix;
        this.objectMapper = objectMapper;
    }

    public void createTables(boolean deleteIfExists) {
        for (TableDefinition tableDefinition : schemaRegistry.getSchema().getTableDefinitions()) {

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
            List<GlobalSecondaryIndex> globalSecondaryIndexes = new ArrayList<>();

            Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(hashField.getDynamoName()).withAttributeType("S"));
            keySchema.add(new KeySchemaElement().withAttributeName(hashField.getDynamoName()).withKeyType(KeyType.HASH));
            if (tableDefinition.getRangeKey() != null) {
                Field field = tableDefinition.getField(tableDefinition.getRangeKey());
                attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getType().equals("String") ? "S" : "N"));
                keySchema.add(new KeySchemaElement().withAttributeName(field.getDynamoName()).withKeyType(KeyType.RANGE));
            }

            for (com.n3twork.dynamap.model.Index index : tableDefinition.getGlobalSecondaryIndexes()) {
                GlobalSecondaryIndex gsi = new GlobalSecondaryIndex()
                        .withIndexName(index.getIndexName())
                        .withProvisionedThroughput(new ProvisionedThroughput()
                                .withReadCapacityUnits(1L)
                                .withWriteCapacityUnits(1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL));
                ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();
                Field field = tableDefinition.getField(index.getHashKey());
                indexKeySchema.add(new KeySchemaElement()
                        .withAttributeName(field.getDynamoName())
                        .withKeyType(KeyType.HASH));
                if (!hasAttributeDefinition(attributeDefinitions, field.getDynamoName())) {
                    attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getType().equals("String") ? "S" : "N"));
                }

                if (index.getRangeKey() != null) {
                    Field rangeField = tableDefinition.getField(index.getRangeKey());
                    indexKeySchema.add(new KeySchemaElement()
                            .withAttributeName(rangeField.getDynamoName())
                            .withKeyType(KeyType.RANGE));

                    if (!hasAttributeDefinition(attributeDefinitions, field.getDynamoName())) {
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getType().equals("String") ? "S" : "N"));
                    }
                }
                gsi.setKeySchema(indexKeySchema);
                globalSecondaryIndexes.add(gsi);
            }


            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableDefinition.getTableName(tablePrefix))
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(1L)
                            .withWriteCapacityUnits(1L));

            if (globalSecondaryIndexes.size() > 0) {
                request = request.withGlobalSecondaryIndexes(globalSecondaryIndexes);
            }
            if (deleteIfExists) {
                TableUtils.deleteTableIfExists(amazonDynamoDB, new DeleteTableRequest().withTableName(tableDefinition.getTableName(tablePrefix)));
            }
            TableUtils.createTableIfNotExists(amazonDynamoDB, request);

        }

    }

    public <T extends DynaMapPersisted> T query(QueryRequest<T> queryRequest, Object migrationContext) {
        Map<String, List<Object>> results = batchQuery(Arrays.asList(queryRequest), migrationContext);
        List<Object> resultList = results.values().iterator().next();
        return (T) resultList.get(0);
    }

    public Map<String, List<Object>> batchQuery(Collection<QueryRequest> queryRequests, Object migrationContext) {

        Map<String, QueryInfo> queryInfos = new HashMap<>();
        Map<String, List<Object>> results = new HashMap<>();

        for (QueryRequest queryRequest : queryRequests) {
            TableDefinition tableDefinition = schemaRegistry.getTableDefinition(queryRequest.getResultClass());
            TableKeysAndAttributes keysAndAttributes = new TableKeysAndAttributes(tableDefinition.getTableName(tablePrefix))
                    .withConsistentRead(queryRequest.isConsistentRead());
            String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
            if (queryRequest.getRangeKeyValue() != null) {
                keysAndAttributes.addHashAndRangePrimaryKey(hashKeyFieldName, queryRequest.getHashKeyValue(), tableDefinition.getRangeKey(), queryRequest.getRangeKeyValue());
            } else {
                keysAndAttributes.addHashOnlyPrimaryKey(hashKeyFieldName, queryRequest.getHashKeyValue());
            }
            QueryInfo queryInfo = new QueryInfo();
            queryInfo.keysAndAttributes = keysAndAttributes;
            queryInfo.tableDefinition = tableDefinition;
            queryInfo.queryRequest = queryRequest;
            queryInfos.put(tableDefinition.getTableName(tablePrefix), queryInfo);
            queryInfo.table = dynamoDB.getTable(tableDefinition.getTableName(tablePrefix));
        }


        Multimap<String, Item> allItems = doBatchGetItem(queryInfos);
        for (QueryInfo queryInfo : queryInfos.values()) {

            Collection<Item> items = allItems.get(queryInfo.tableDefinition.getTableName(tablePrefix));
            List<Object> resultsForTable = results.get(queryInfo.tableDefinition.getTableName());
            if (resultsForTable == null) {
                resultsForTable = new ArrayList<>();
                results.put(queryInfo.tableDefinition.getTableName(), resultsForTable);
            }
            for (Item item : items) {
                resultsForTable.add(buildObjectFromDynamoItem(item, queryInfo, migrationContext));
            }
        }
        return results;
    }

    public void save(DynaMapPersisted object, DynamoRateLimiter writeLimiter) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(object.getClass());
        putObject(object, tableDefinition, writeLimiter);
    }

    public <T extends DynaMapPersisted> T update(Updates<T> updates) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(updates.getTableName());
        UpdateItemSpec updateItemSpec = getUpdateItemSpec(updates, tableDefinition);
        Table table = dynamoDB.getTable(tableDefinition.getTableName(tablePrefix));

        logger.debug("About to submit DynamoDB Update: Update expression: {}, Conditional expression: {}, Values {}, Names: {}", updateItemSpec.getUpdateExpression(), updateItemSpec.getConditionExpression(), updateItemSpec.getValueMap(), updateItemSpec.getNameMap());
        try {
            updateItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            if (logger.isDebugEnabled()) {
                logger.debug("UpdateItemOutcome: " + updateItemOutcome.getItem().toJSONPretty());
            }

            Class beanClass = Class.forName(tableDefinition.getPackageName() + "." + tableDefinition.getType() + "Bean");
            return (T) objectMapper.convertValue(updateItemOutcome.getItem().asMap(), beanClass);

        } catch (Exception e) {
            String keyComponents = updateItemSpec.getKeyComponents().stream().map(Object::toString).collect(Collectors.joining(","));
            logger.error("Error updating item: Key: " + keyComponents + " Update expression:" + updateItemSpec.getUpdateExpression() + " Conditional expression: " + updateItemSpec.getConditionExpression() + " Values: " + updateItemSpec.getValueMap() + " Names: " + updateItemSpec.getNameMap(), e);
            throw new RuntimeException(e);
        }


    }


    private static class QueryInfo {

        public TableKeysAndAttributes keysAndAttributes;
        public TableDefinition tableDefinition;
        public QueryRequest queryRequest;
        public Table table;

    }

    private Multimap<String, Item> doBatchGetItem(Map<String, QueryInfo> queryInfos) {
        Multimap<String, Item> results = ArrayListMultimap.create();
        try {
            TableKeysAndAttributes[] tableKeysAndAttributes = new TableKeysAndAttributes[queryInfos.size()];
            int index = 0;
            for (QueryInfo queryInfo : queryInfos.values()) {
                tableKeysAndAttributes[index] = queryInfo.keysAndAttributes;
            }

            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(ReturnConsumedCapacity.TOTAL, tableKeysAndAttributes);

            int unprocessedKeyCount;
            //todo: need to add exponential backoff for unprocessed items and a termination condition
            do {

                if (outcome.getBatchGetItemResult().getConsumedCapacity() != null) {
                    for (ConsumedCapacity consumedCapacity : outcome.getBatchGetItemResult().getConsumedCapacity()) {
                        setConsumedUnits(queryInfos, consumedCapacity, false);
                    }
                }

                Map<String, List<Item>> tableItems = outcome.getTableItems();
                for (String tableName : tableItems.keySet()) {
                    List<Item> items = tableItems.get(tableName);
                    for (Item item : items) {
                        results.put(tableName, item);
                    }
                }
                // Check for unprocessed keys which could happen if it exceeds provisioned
                // throughput or reach the limit on response size.
                Map<String, KeysAndAttributes> unprocessedKeys = outcome.getUnprocessedKeys();

                unprocessedKeyCount = outcome.getUnprocessedKeys().size();
                if (unprocessedKeyCount != 0) {
                    initAndAcquire(queryInfos, false);
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
                }

            } while (unprocessedKeyCount > 0);

            return results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setConsumedUnits(Map<String, QueryInfo> queryInfos, ConsumedCapacity consumedCapacity, boolean write) {
        QueryInfo queryInfo = queryInfos.get(consumedCapacity.getTableName());
        DynamoRateLimiter rateLimiter = write ? queryInfo.queryRequest.getWriteRateLimiter() : queryInfo.queryRequest.getReadRateLimiter();
        if (rateLimiter != null) {
            rateLimiter.setConsumedCapacity(consumedCapacity);
        }
    }

    private void initAndAcquire(Map<String, QueryInfo> queryInfos, boolean write) {
        for (QueryInfo queryInfo : queryInfos.values()) {
            DynamoRateLimiter rateLimiter = write ? queryInfo.queryRequest.getWriteRateLimiter() : queryInfo.queryRequest.getReadRateLimiter();
            if (rateLimiter != null) {
                rateLimiter.init(queryInfo.table);
                rateLimiter.acquire();
            }
        }
    }

    private Object buildObjectFromDynamoItem(Item item, QueryInfo queryInfo, Object migrationContext) {
        if (item == null) {
            return null;
        }

        Object result;
        int currentVersion = item.getInt(Schema.SCHEMA_VERSION_FIELD);
        String className = queryInfo.queryRequest.getResultClass().getName();
        try {
            if (currentVersion != queryInfo.tableDefinition.getVersion()) {
                for (Migration migration : schemaRegistry.getMigrations(queryInfo.tableDefinition.getTableName())) {
                    if (migration.getVersion() > currentVersion) {
                        migration.migrate(item, currentVersion, migrationContext);
                    }
                }
                for (Migration migration : schemaRegistry.getMigrations(queryInfo.tableDefinition.getTableName())) {
                    if (migration.getVersion() > currentVersion) {
                        migration.postMigration(item, currentVersion, migrationContext);
                    }
                }
                item = item.withInt(Schema.SCHEMA_VERSION_FIELD, queryInfo.tableDefinition.getVersion());
                result = objectMapper.convertValue(item.asMap(), Class.forName(className));
                putObject(result, queryInfo.tableDefinition, queryInfo.queryRequest.getWriteRateLimiter());
            } else {
                result = objectMapper.convertValue(item.asMap(), Class.forName(className));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private void putObject(Object object, TableDefinition tableDefinition, DynamoRateLimiter writeLimiter) {
        try {
            Map<String, Object> map = objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
            });
            Item item = new Item().withInt(Schema.SCHEMA_VERSION_FIELD, tableDefinition.getVersion());
            Type type = tableDefinition.getTypes().stream().filter(t -> t.getName().equals(tableDefinition.getType())).findFirst().get();
            for (Field field : type.getFields()) {
                if (field.getMultiValue() != null) {
                    if (field.getMultiValue().equals("Map")) {
                        item.withMap(field.getDynamoName(), (Map) map.get(field.getDynamoName()));
                    } else if (field.getMultiValue().equals("Set")) {
                        if (field.getType().equals("String")) {
                            item.withStringSet(field.getDynamoName(), (String) map.get(field.getDynamoName()));
                        } else if (field.isNumber()) {
                            item.withNumberSet(field.getDynamoName(), (Number) map.get(field.getDynamoName()));
                        } else {
                            throw new RuntimeException("Invalid type for Set: " + field.getName() + ":" + field.getType());
                        }
                    } else if (field.getMultiValue().equals("List")) {
                        item.withList(field.getDynamoName(), map.get(field.getDynamoName()));
                    }
                } else if (field.getType().equals("String")) {
                    item.withString(field.getDynamoName(), (String) map.get(field.getDynamoName()));
                } else if (field.isNumber()) {
                    item.withNumber(field.getDynamoName(), (Number) map.get(field.getDynamoName()));
                } else {
                    item.with(field.getDynamoName(), map.get(field.getDynamoName()));
                }
            }

            Pair<String, Object> primaryKeyValue = tableDefinition.getPrimaryKeyValue(object);
            if (primaryKeyValue.getRight() != null) {
                item.withPrimaryKey(tableDefinition.getHashKey(), primaryKeyValue.getLeft(), tableDefinition.getRangeKey(), primaryKeyValue.getRight());
            } else {
                item.withPrimaryKey(tableDefinition.getHashKey(), primaryKeyValue.getLeft());
            }
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withReturnValues(ReturnValue.NONE);
            Table table = dynamoDB.getTable(tableDefinition.getTableName(tablePrefix)); //TODO: this should be cached
            try {
                if (writeLimiter != null) {
                    writeLimiter.init(table);
                    writeLimiter.acquire();
                }
                PutItemOutcome outcome = table.putItem(putItemSpec);
                if (writeLimiter != null) {
                    writeLimiter.setConsumedCapacity(outcome.getPutItemResult().getConsumedCapacity());
                }
            } catch (RuntimeException e) {
                logger.error("Error putting item:" + putItemSpec.getItem().toJSON() + " Conditional expression: " + putItemSpec.getConditionExpression() + " Values: " + putItemSpec.getValueMap() + " Names: " + putItemSpec.getNameMap(), e);
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasAttributeDefinition(Collection<AttributeDefinition> attributeDefinitions, String name) {
        return attributeDefinitions.stream().anyMatch(d -> d.getAttributeName().equals(name));
    }

    private UpdateItemSpec getUpdateItemSpec(Updates updates, TableDefinition tableDefinition) {
        String updateExpression = updates.getUpdateExpression();
        String conditionalExpression = updates.getConditionalExpression();
        UpdateItemSpec result = new UpdateItemSpec().withReturnValues(ReturnValue.ALL_NEW);
        Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
        if (updates.getRangeKeyValue() != null) {
            Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
            result.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue(), rangeField.getDynamoName(), updates.getRangeKeyValue());
        } else {
            result.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue());
        }
        if (null != conditionalExpression && !"".equals(conditionalExpression)) {
            result = result.withConditionExpression(conditionalExpression);
        }
        if (null != updateExpression && !"".equals(updateExpression)) {
            result = result.withUpdateExpression(updateExpression);
        }
        if (!updates.withExpression().getNameMap().isEmpty()) {
            result = result.withNameMap(updates.withExpression().getNameMap());
        }
        if (!updates.withExpression().getValueMap().isEmpty()) {
            result = result.withValueMap(updates.withExpression().getValueMap());
        }
        return result;
    }

}
