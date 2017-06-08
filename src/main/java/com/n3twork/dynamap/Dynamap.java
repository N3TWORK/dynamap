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
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Dynamap {

    private static final Logger logger = LoggerFactory.getLogger(Dynamap.class);

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;
    private final SchemaRegistry schemaRegistry;
    private String prefix;
    private ObjectMapper objectMapper;

    private static final int MAX_BATCH_SIZE = 25;

    public Dynamap(AmazonDynamoDB amazonDynamoDB, SchemaRegistry schemaRegistry) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
        this.schemaRegistry = schemaRegistry;
        this.objectMapper = new ObjectMapper();
    }

    public Dynamap withObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    public Dynamap withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
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

            if (tableDefinition.getGlobalSecondaryIndexes() != null) {
                for (com.n3twork.dynamap.model.Index index : tableDefinition.getGlobalSecondaryIndexes()) {
                    GlobalSecondaryIndex gsi = new GlobalSecondaryIndex()
                            .withIndexName(index.getIndexName(prefix))
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

                        if (!hasAttributeDefinition(attributeDefinitions, rangeField.getDynamoName())) {
                            attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeField.getDynamoName()).withAttributeType(rangeField.getType().equals("String") ? "S" : "N"));
                        }
                    }
                    gsi.setKeySchema(indexKeySchema);
                    globalSecondaryIndexes.add(gsi);
                }
            }

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableDefinition.getTableName(prefix))
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(1L)
                            .withWriteCapacityUnits(1L));

            if (globalSecondaryIndexes.size() > 0) {
                request = request.withGlobalSecondaryIndexes(globalSecondaryIndexes);
            }
            if (deleteIfExists) {
                TableUtils.deleteTableIfExists(amazonDynamoDB, new DeleteTableRequest().withTableName(tableDefinition.getTableName(prefix)));
            }
            TableUtils.createTableIfNotExists(amazonDynamoDB, request);

        }

    }

    public <T extends DynamapRecordBean> T getObject(GetObjectRequest<T> getObjectRequest, Object migrationContext) {
        Map<String, List<Object>> results = batchGetObject(Arrays.asList(getObjectRequest), migrationContext);
        List<Object> resultList = results.values().iterator().next();
        if (resultList.size() > 0) {
            return (T) resultList.get(0);
        }
        return null;
    }

    // TODO:
    // for each request GetObjectRequest there is info about read consistency, read rate limiter and write rate limiter
    // but for each iteration the above values are being overwritten so only the the values from the last GetObjectRequest will be used.
    // anyway the above mentiond values make sense only for the full table, so they should passed in another object
    // or just be aware of the actual behavior
    public Map<String, List<Object>> batchGetObject(Collection<GetObjectRequest> getObjectRequests, Object migrationContext) {

        Map<String, GetItemInfo> queryInfos = new HashMap<>();
        Map<String, List<Object>> results = new HashMap<>();

        for (GetObjectRequest getObjectRequest : getObjectRequests) {
            TableDefinition tableDefinition = schemaRegistry.getTableDefinition(getObjectRequest.getResultClass());
            String tableName = tableDefinition.getTableName(prefix);
            TableKeysAndAttributes keysAndAttributes;
            if (queryInfos.get(tableName) != null) {
                keysAndAttributes = queryInfos.get(tableName).keysAndAttributes;
            }
            else {
                keysAndAttributes = new TableKeysAndAttributes(tableName)
                        .withConsistentRead(getObjectRequest.isConsistentRead());
            }

            String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
            if (getObjectRequest.getRangeKeyValue() != null) {
                String rangeKeyFieldName = tableDefinition.getField(tableDefinition.getRangeKey()).getDynamoName();
                keysAndAttributes.addHashAndRangePrimaryKey(hashKeyFieldName, getObjectRequest.getHashKeyValue(), rangeKeyFieldName, getObjectRequest.getRangeKeyValue());
            } else {
                keysAndAttributes.addHashOnlyPrimaryKey(hashKeyFieldName, getObjectRequest.getHashKeyValue());
            }
            GetItemInfo getItemInfo = new GetItemInfo();
            getItemInfo.keysAndAttributes = keysAndAttributes;
            getItemInfo.tableDefinition = tableDefinition;
            getItemInfo.getObjectRequest = getObjectRequest;
            queryInfos.put(tableName, getItemInfo);
            getItemInfo.table = dynamoDB.getTable(tableName);
        }


        Multimap<String, Item> allItems = doBatchGetItem(queryInfos);
        for (GetItemInfo getItemInfo : queryInfos.values()) {

            Collection<Item> items = allItems.get(getItemInfo.tableDefinition.getTableName(prefix));
            List<Object> resultsForTable = results.get(getItemInfo.tableDefinition.getTableName());
            if (resultsForTable == null) {
                resultsForTable = new ArrayList<>();
                results.put(getItemInfo.tableDefinition.getTableName(), resultsForTable);
            }
            for (Item item : items) {
                resultsForTable.add(buildObjectFromDynamoItem(item, getItemInfo.tableDefinition,
                        getItemInfo.getObjectRequest.getResultClass(), getItemInfo.getObjectRequest.getWriteRateLimiter(),
                        migrationContext, true));
            }
        }
        return results;
    }

    public <T extends DynamapRecordBean> List<T> query(QueryRequest<T> queryRequest, Object migrationContext) {
        List<T> results = new ArrayList<>();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(queryRequest.getResultClass());
        Table table = dynamoDB.getTable(tableDefinition.getTableName(prefix));
        QuerySpec querySpec = new QuerySpec().withHashKey(tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue())
                .withRangeKeyCondition(queryRequest.getRangeKeyCondition())
                .withConsistentRead(queryRequest.isConsistentRead())
                .withQueryFilters(queryRequest.getQueryFilters())
                .withScanIndexForward(queryRequest.isScanIndexForward())
                .withMaxResultSize(queryRequest.getLimit());

        ItemCollection<QueryOutcome> items;
        if (queryRequest.getIndex() != null && tableDefinition.getGlobalSecondaryIndexes() != null) {
            com.n3twork.dynamap.model.Index indexDef = tableDefinition.getGlobalSecondaryIndexes().stream().filter(i -> i.getIndexName().equals(queryRequest.getIndex().getName())).findFirst().get();
            String indexName = indexDef.getIndexName(prefix);
            Index index = table.getIndex(indexDef.getIndexName(prefix));
            querySpec.withHashKey(tableDefinition.getField(indexDef.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue());
            initAndAcquire(queryRequest.getReadRateLimiter(), table, indexName);
            items = index.query(querySpec);
        } else {
            querySpec.withHashKey(tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue());
            initAndAcquire(queryRequest.getReadRateLimiter(), table, null);
            items = table.query(querySpec);
        }

        items.registerLowLevelResultListener(new LowLevelResultListener<QueryOutcome>() {
            @Override
            public void onLowLevelResult(QueryOutcome queryOutcome) {
                DynamoRateLimiter dynamoRateLimiter = queryRequest.getReadRateLimiter();
                if (dynamoRateLimiter != null) {
                    dynamoRateLimiter.setConsumedCapacity(queryOutcome.getQueryResult().getConsumedCapacity());
                    dynamoRateLimiter.acquire();
                }
            }
        });
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(buildObjectFromDynamoItem(iterator.next(), tableDefinition, queryRequest.getResultClass(), null, migrationContext, false));
        }

        return results;
    }

    // TODO:
    // Ultra basic scan implementation
    // - Make a proper scan object (ScanRequest) and apply corresponding options
    // - Do tests
    public <T extends DynamapRecordBean> List<T> scan(QueryRequest<T> queryRequest, Object migrationContext) {
        List<T> results = new ArrayList<>();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(queryRequest.getResultClass());
        Table table = dynamoDB.getTable(tableDefinition.getTableName(prefix));

        ScanSpec spec = new ScanSpec();

        Iterator<Item> iterator = table.scan(spec).iterator();

        while (iterator.hasNext()) {
            results.add(buildObjectFromDynamoItem(iterator.next(), tableDefinition, queryRequest.getResultClass(), null, migrationContext, false));
        }

        return results;
    }

    public void save(DynamapRecordBean object, DynamoRateLimiter writeLimiter) {
        save(object, true, false, writeLimiter);

    }

    public void save(DynamapRecordBean object, boolean overwrite, DynamoRateLimiter writeLimiter) {
        save(object, overwrite, false, writeLimiter);
    }

    public void save(DynamapRecordBean object, boolean overwrite, boolean disableOptimisticLocking, DynamoRateLimiter writeLimiter) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(object.getClass());
        putObject(object, tableDefinition, overwrite, disableOptimisticLocking, writeLimiter);
    }

    public <T extends DynamapPersisted> T update(Updates<T> updates) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(updates.getTableName());
        UpdateItemSpec updateItemSpec = getUpdateItemSpec(updates, tableDefinition);
        Table table = dynamoDB.getTable(tableDefinition.getTableName(prefix));

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

    private static class GetItemInfo {
        public TableKeysAndAttributes keysAndAttributes;
        public TableDefinition tableDefinition;
        public GetObjectRequest getObjectRequest;
        public Table table;
    }

    private Multimap<String, Item> doBatchGetItem(Map<String, GetItemInfo> queryInfos) {
        Multimap<String, Item> results = ArrayListMultimap.create();
        try {
            TableKeysAndAttributes[] tableKeysAndAttributes = new TableKeysAndAttributes[queryInfos.size()];
            int index = 0;
            for (GetItemInfo getItemInfo : queryInfos.values()) {
                tableKeysAndAttributes[index++] = getItemInfo.keysAndAttributes;
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

    private void setConsumedUnits(Map<String, GetItemInfo> queryInfos, ConsumedCapacity consumedCapacity, boolean write) {
        GetItemInfo getItemInfo = queryInfos.get(consumedCapacity.getTableName());
        DynamoRateLimiter rateLimiter = write ? getItemInfo.getObjectRequest.getWriteRateLimiter() : getItemInfo.getObjectRequest.getReadRateLimiter();
        if (rateLimiter != null) {
            rateLimiter.setConsumedCapacity(consumedCapacity);
        }
    }

    private void initAndAcquire(Map<String, GetItemInfo> queryInfos, boolean write) {
        for (GetItemInfo getItemInfo : queryInfos.values()) {
            DynamoRateLimiter rateLimiter = write ? getItemInfo.getObjectRequest.getWriteRateLimiter() : getItemInfo.getObjectRequest.getReadRateLimiter();
            if (rateLimiter != null) {
                rateLimiter.init(getItemInfo.table);
                rateLimiter.acquire();
            }
        }
    }

    private void initAndAcquire(DynamoRateLimiter readRateLimiter, Table table, String indexName) {
        if (readRateLimiter != null) {
            readRateLimiter.init(table, indexName);
        }
    }

    private <T extends DynamapRecordBean> T buildObjectFromDynamoItem(Item item, TableDefinition tableDefinition, Class<T> resultClass, DynamoRateLimiter writeRateLimiter, Object migrationContext, boolean writeBack) {
        if (item == null) {
            return null;
        }

        T result;
        int currentVersion = item.getInt(Schema.SCHEMA_VERSION_FIELD);
        try {
            if (currentVersion != tableDefinition.getVersion()) {
                for (Migration migration : schemaRegistry.getMigrations(tableDefinition.getTableName())) {
                    if (migration.getVersion() > currentVersion) {
                        migration.migrate(item, currentVersion, migrationContext);
                    }
                }
                for (Migration migration : schemaRegistry.getMigrations(tableDefinition.getTableName())) {
                    if (migration.getVersion() > currentVersion) {
                        migration.postMigration(item, currentVersion, migrationContext);
                    }
                }
                item = item.withInt(Schema.SCHEMA_VERSION_FIELD, tableDefinition.getVersion());
                result = objectMapper.convertValue(item.asMap(), resultClass);
                if (writeBack) {
                    putObject(result, tableDefinition, true, false, writeRateLimiter);
                }
            } else {
                result = objectMapper.convertValue(item.asMap(), resultClass);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private <T extends DynamapRecordBean> Item buildDynamoItemFromObject(T object, TableDefinition tableDefinition, boolean disableOptimisticLocking) {
        Map<String, Object> map = objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
        });
        Item item = new Item().withInt(Schema.SCHEMA_VERSION_FIELD, tableDefinition.getVersion());

        if (!disableOptimisticLocking && tableDefinition.isOptimisticLocking()) {
            int revision = (int) map.getOrDefault(Schema.REVISION_FIELD, 0);
            item.withInt(Schema.REVISION_FIELD, revision + 1);
        }

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

        String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
        if (object.getRangeKeyValue() != null) {
            String rangeKeyFieldName = tableDefinition.getField(tableDefinition.getRangeKey()).getDynamoName();
            item.withPrimaryKey(hashKeyFieldName, object.getHashKeyValue(), rangeKeyFieldName, object.getRangeKeyValue());
        } else {
            item.withPrimaryKey(hashKeyFieldName, object.getHashKeyValue());
        }

        return item;
    }

    private <T extends DynamapRecordBean> Item buildDynamoItemFromObject(T object, TableDefinition tableDefinition) {
        return buildDynamoItemFromObject(object, tableDefinition, false);
    }

    private <T extends DynamapRecordBean> void putObject(T object, TableDefinition tableDefinition, boolean overwrite, boolean disableOptimisticLocking, DynamoRateLimiter writeLimiter) {
        try {
            Item item = buildDynamoItemFromObject(object, tableDefinition, disableOptimisticLocking);
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withReturnValues(ReturnValue.NONE);
            String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
            ValueMap valueMap = new ValueMap();
            List<String> conditionalExpressions = new ArrayList<>();
            if (!overwrite) {
                conditionalExpressions.add("attribute_not_exists("+hashKeyFieldName+")");
            }

            if (!disableOptimisticLocking && tableDefinition.isOptimisticLocking()) {
                // value is incremented in buildDynamoItemFromObject, so here i must get the original value
                int revision = item.getInt(Schema.REVISION_FIELD) - 1;
                if (revision > 0) {
                    conditionalExpressions.add(Schema.REVISION_FIELD + "=:val0");
                    valueMap.withInt(":val0", revision);
                }
            }

            if (conditionalExpressions.size() > 0) {
                putItemSpec.withConditionExpression(String.join(" AND ", conditionalExpressions));
                if (valueMap.size() > 0 ) {
                    putItemSpec.withValueMap(valueMap);
                }
            }

            Table table = dynamoDB.getTable(tableDefinition.getTableName(prefix)); //TODO: this should be cached
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
        DynamoExpressionBuilder expressionBuilder = updates.getUpdateExpression(objectMapper);
        updates.addConditionalExpression(expressionBuilder);
        UpdateItemSpec result = new UpdateItemSpec().withReturnValues(ReturnValue.ALL_NEW);
        Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
        if (updates.getRangeKeyValue() != null) {
            Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
            result.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue(), rangeField.getDynamoName(), updates.getRangeKeyValue());
        } else {
            result.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue());
        }
        String conditionalExpression = expressionBuilder.buildConditionalExpression();
        if (null != conditionalExpression && !"".equals(conditionalExpression)) {
            result = result.withConditionExpression(conditionalExpression);
        }
        String updateExpression = expressionBuilder.buildUpdateExpression();
        if (null != updateExpression && !"".equals(updateExpression)) {
            result = result.withUpdateExpression(updateExpression);
        }
        if (!expressionBuilder.getNameMap().isEmpty()) {
            result = result.withNameMap(expressionBuilder.getNameMap());
        }
        if (!expressionBuilder.getValueMap().isEmpty()) {
            result = result.withValueMap(expressionBuilder.getValueMap());
        }
        return result;
    }


    public void delete(DeleteRequest deleteRequest) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(deleteRequest.getResultClass());
        Table table = dynamoDB.getTable(tableDefinition.getTableName(prefix));

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec();
        Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
        if (deleteRequest.getRangeKeyValue() != null) {
            Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
            deleteItemSpec.withPrimaryKey(hashField.getDynamoName(), deleteRequest.getHashKeyValue(), rangeField.getDynamoName(), deleteRequest.getRangeKeyValue());
        } else {
            deleteItemSpec.withPrimaryKey(hashField.getDynamoName(), deleteRequest.getHashKeyValue());
        }

       table.deleteItem(deleteItemSpec);
    }

    public <T extends DynamapRecordBean> void batchSave(List<T> objects, Map<String, DynamoRateLimiter> writeLimiterMap) {
        final List<List<T>> objectsBatch = Lists.partition(objects, MAX_BATCH_SIZE);
        for (List<T> batch : objectsBatch) {
            logger.debug("Sending batch to save of size: {}", batch.size());
            doBatchWriteItem(batch, writeLimiterMap);
        }
    }

    public <T extends DynamapRecordBean> void doBatchWriteItem(List<T> objects, Map<String, DynamoRateLimiter> writeLimiterMap) {
        Map<String, TableWriteItems> tableWriteItems = new HashMap<>();

        for (DynamapRecordBean object: objects) {
            TableDefinition tableDefinition = schemaRegistry.getTableDefinition(object.getClass());
            Item item = buildDynamoItemFromObject(object, tableDefinition);

            String tableName = tableDefinition.getTableName(prefix);
            TableWriteItems writeItems = tableWriteItems.getOrDefault(tableName, new TableWriteItems(tableName));
            tableWriteItems.put(tableName, writeItems.addItemToPut(item));
        }

        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec()
                .withTableWriteItems(tableWriteItems.values().toArray(new TableWriteItems[tableWriteItems.size()]));

        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(batchWriteItemSpec);

        while (outcome.getUnprocessedItems().size() > 0) {
            if (writeLimiterMap != null && outcome.getBatchWriteItemResult().getConsumedCapacity() != null) {
                // need better testing
                for (ConsumedCapacity consumedCapacity : outcome.getBatchWriteItemResult().getConsumedCapacity()) {
                    DynamoRateLimiter rateLimiter = writeLimiterMap.get(consumedCapacity.getTableName());
                    rateLimiter.setConsumedCapacity(consumedCapacity);
                    logger.debug("Set rate limiter capacity {}", consumedCapacity.getCapacityUnits());
                    Table table = dynamoDB.getTable(consumedCapacity.getTableName()); //TODO: this should be cached
                    rateLimiter.init(table);
                    rateLimiter.acquire();
                }
            }

            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            logger.debug("Retrieving unprocessed items, size: {}", unprocessedItems.size());
            outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
        }

        logger.debug("doBatchWriteItem done");
    }

}
