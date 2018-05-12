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
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
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
    private final Map<String, Table> tableCache = new HashMap<>();
    private String prefix;
    private ObjectMapper objectMapper;
    private Map<String, CreateTableRequest> createTableRequests = new HashMap<>();

    private static final int MAX_BATCH_SIZE = 25;
    private static final int MAX_BATCH_GET_SIZE = 100;

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

    public SchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }

    public void createTables(boolean deleteIfExists) {
        for (TableDefinition tableDefinition : schemaRegistry.getSchema().getTableDefinitions()) {

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
            List<GlobalSecondaryIndex> globalSecondaryIndexes = new ArrayList<>();
            List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<>();

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
                            .withIndexName(index.getIndexName())
                            .withProvisionedThroughput(new ProvisionedThroughput()
                                    .withReadCapacityUnits(1L)
                                    .withWriteCapacityUnits(1L))
                            .withProjection(new Projection().withProjectionType(ProjectionType.ALL));
                    ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<>();
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

            if (tableDefinition.getLocalSecondaryIndexes() != null) {
                for (com.n3twork.dynamap.model.Index index : tableDefinition.getLocalSecondaryIndexes()) {
                    LocalSecondaryIndex lsi = new LocalSecondaryIndex()
                            .withIndexName(index.getIndexName())
                            .withProjection(new Projection().withProjectionType(ProjectionType.ALL));
                    ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<>();
                    Field field = tableDefinition.getField(index.getHashKey());
                    indexKeySchema.add(new KeySchemaElement()
                            .withAttributeName(field.getDynamoName())
                            .withKeyType(KeyType.HASH));
                    if (!hasAttributeDefinition(attributeDefinitions, field.getDynamoName())) {
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getType().equals("String") ? "S" : "N"));
                    }

                    Field rangeField = tableDefinition.getField(index.getRangeKey());
                    indexKeySchema.add(new KeySchemaElement()
                            .withAttributeName(rangeField.getDynamoName())
                            .withKeyType(KeyType.RANGE));

                    if (!hasAttributeDefinition(attributeDefinitions, rangeField.getDynamoName())) {
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeField.getDynamoName()).withAttributeType(rangeField.getType().equals("String") ? "S" : "N"));
                    }

                    lsi.setKeySchema(indexKeySchema);
                    localSecondaryIndexes.add(lsi);
                }
            }

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableDefinition.getTableName(prefix))
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(1L)
                            .withWriteCapacityUnits(1L));

            createTableRequests.put(tableDefinition.getTableName(), request);

            if (globalSecondaryIndexes.size() > 0) {
                request = request.withGlobalSecondaryIndexes(globalSecondaryIndexes);
            }

            if (localSecondaryIndexes.size() > 0) {
                request = request.withLocalSecondaryIndexes(localSecondaryIndexes);
            }

            if (deleteIfExists) {
                TableUtils.deleteTableIfExists(amazonDynamoDB, new DeleteTableRequest().withTableName(tableDefinition.getTableName(prefix)));
            }
            TableUtils.createTableIfNotExists(amazonDynamoDB, request);

        }

    }

    // Used for tests, allows to easily create a table with suffix using the schema an existing table
    public boolean createTableFromExisting(String baseTableName, String newTableName, boolean deleteIfExists) {
        CreateTableRequest createTableRequest = createTableRequests.get(baseTableName);
        String fullNewTableName = prefix + newTableName;
        createTableRequest.withTableName(fullNewTableName);
        if (deleteIfExists) {
            TableUtils.deleteTableIfExists(amazonDynamoDB, new DeleteTableRequest().withTableName(fullNewTableName));
        }
        return TableUtils.createTableIfNotExists(amazonDynamoDB, createTableRequest);
    }

    public <T extends DynamapRecordBean> T getObject(GetObjectRequest<T> getObjectRequest, Object migrationContext) {
        Collection<GetObjectRequest<T>> requests = new ArrayList<>();
        requests.add(getObjectRequest);
        BatchGetObjectRequest<T> batchGetObjectRequest = new BatchGetObjectRequest().withGetObjectRequests(requests).withMigrationContext(migrationContext);
        Map<Class, List<Object>> results = batchGetObject(batchGetObjectRequest);
        List<Object> resultList = results.values().iterator().next();
        if (resultList.size() > 0) {
            return (T) resultList.get(0);
        }
        return null;
    }

    public <T extends DynamapRecordBean> T getObject(GetObjectRequest<T> getObjectRequest, ReadWriteRateLimiterPair rateLimiters, Object migrationContext) {
        BatchGetObjectRequest<T> batchGetObjectRequest = new BatchGetObjectRequest<T>()
                .withGetObjectRequests(Arrays.asList(getObjectRequest))
                .withRateLimiters(ImmutableMap.of(getObjectRequest.getResultClass(), rateLimiters))
                .withMigrationContext(migrationContext);
        Map<Class, List<Object>> results = batchGetObject(batchGetObjectRequest);
        List<Object> resultList = results.values().iterator().next();
        if (resultList.size() > 0) {
            return (T) resultList.get(0);
        }
        return null;
    }

    public Map<Class, List<Object>> batchGetObject(BatchGetObjectRequest batchGetObjectRequest) {
        Map<String, ReadWriteRateLimiterPair> rateLimitersByTable = new HashMap<>();
        Map<Class, ReadWriteRateLimiterPair> rateLimiters = batchGetObjectRequest.getRateLimiters();
        if (batchGetObjectRequest.getRateLimiters() != null) {
            for (Class resultClass : rateLimiters.keySet()) {
                rateLimitersByTable.put(schemaRegistry.getTableDefinition(resultClass).getTableName(prefix), rateLimiters.get(resultClass));
            }
        }

        List<List<GetObjectRequest>> partitions = Lists.partition(new ArrayList<>(batchGetObjectRequest.getGetObjectRequests()), MAX_BATCH_GET_SIZE);
        Map<Class, List<Object>> results = new HashMap<>();
        int totalProgress = 0;
        for (List<GetObjectRequest> getObjectRequestBatch : partitions) {

            Map<String, GetItemInfo> queryInfos = new HashMap<>();

            for (GetObjectRequest getObjectRequest : getObjectRequestBatch) {
                TableDefinition tableDefinition = schemaRegistry.getTableDefinition(getObjectRequest.getResultClass());
                String tableName = tableDefinition.getTableName(prefix, getObjectRequest.getSuffix());
                TableKeysAndAttributes keysAndAttributes;
                if (queryInfos.get(tableName) != null) {
                    keysAndAttributes = queryInfos.get(tableName).keysAndAttributes;
                } else {
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
                getItemInfo.table = getTable(tableName);
            }

            Multimap<String, Item> allItems = doBatchGetItem(queryInfos, rateLimitersByTable, totalProgress, batchGetObjectRequest.getProgressCallback());
            totalProgress += allItems.values().size();
            for (GetItemInfo getItemInfo : queryInfos.values()) {

                Collection<Item> items = allItems.get(getItemInfo.tableDefinition.getTableName(prefix, getItemInfo.getObjectRequest.getSuffix()));
                List<Object> resultsForClass = results.get(getItemInfo.getObjectRequest.getResultClass());
                if (resultsForClass == null) {
                    resultsForClass = new ArrayList<>();
                    results.put(getItemInfo.getObjectRequest.getResultClass(), resultsForClass);
                }
                for (Item item : items) {
                    DynamoRateLimiter writeLimiter = null;
                    if (rateLimiters != null) {
                        ReadWriteRateLimiterPair pair = rateLimiters.get(getItemInfo.getObjectRequest.getResultClass());
                        if (pair != null) {
                            writeLimiter = pair.getWriteLimiter();
                        }
                    }
                    resultsForClass.add(((Object) buildObjectFromDynamoItem(item, getItemInfo.tableDefinition,
                            getItemInfo.getObjectRequest.getResultClass(), writeLimiter,
                            batchGetObjectRequest.getMigrationContext(), batchGetObjectRequest.isWriteMigrationChange(), false, getItemInfo.getObjectRequest.getSuffix())));
                }
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public <T extends DynamapRecordBean> List<T> batchGetObjectSingleCollection(BatchGetObjectRequest<T> batchGetObjectRequest) {
        Collection<GetObjectRequest<T>> getObjectRequests = batchGetObjectRequest.getGetObjectRequests();
        if (getObjectRequests.size() == 0) {
            return Collections.emptyList();
        }
        // ensure that only one result class has been specified
        GetObjectRequest<T> getObjectRequest = getObjectRequests.iterator().next();
        String resultClass = getObjectRequest.getResultClass().getCanonicalName();
        if (getObjectRequests.stream().anyMatch(r -> !r.getResultClass().getCanonicalName().equals(resultClass))) {
            throw new IllegalArgumentException("More than one ResultClass has been specified");
        }
        if (batchGetObjectRequest.getReadWriteRateLimiterPair() != null) {
            batchGetObjectRequest.withRateLimiters(ImmutableMap.of(getObjectRequest.getResultClass(), batchGetObjectRequest.getReadWriteRateLimiterPair()));
        }
        return (List<T>) (Object) batchGetObject(batchGetObjectRequest).get(getObjectRequest.getResultClass());
    }

    public <T extends DynamapRecordBean> List<T> query(QueryRequest<T> queryRequest) {
        List<T> results = new ArrayList<>();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(queryRequest.getResultClass());
        Table table = getTable(tableDefinition.getTableName(prefix, queryRequest.getSuffix()));
        QuerySpec querySpec = new QuerySpec().withHashKey(tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue())
                .withRangeKeyCondition(queryRequest.getRangeKeyCondition())
                .withConsistentRead(queryRequest.isConsistentRead())
                .withQueryFilters(queryRequest.getQueryFilters())
                .withScanIndexForward(queryRequest.isScanIndexForward())
                .withMaxResultSize(queryRequest.getLimit());

        ItemCollection<QueryOutcome> items;
        if (queryRequest.getIndex() != null) {
            com.n3twork.dynamap.model.Index indexDef = null;
            if (tableDefinition.getGlobalSecondaryIndexes() != null) {
                indexDef = tableDefinition.getGlobalSecondaryIndexes().stream().filter(i -> i.getIndexName().equals(queryRequest.getIndex().getName())).findFirst().get();
            } else if (tableDefinition.getLocalSecondaryIndexes() != null) {
                indexDef = tableDefinition.getLocalSecondaryIndexes().stream().filter(i -> i.getIndexName().equals(queryRequest.getIndex().getName())).findFirst().get();
            }
            String indexName = indexDef.getIndexName();
            Index index = table.getIndex(indexDef.getIndexName());
            querySpec.withHashKey(tableDefinition.getField(indexDef.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue());
            initAndAcquire(queryRequest.getReadRateLimiter(), table, indexName);
            items = index.query(querySpec);
        } else {
            querySpec.withHashKey(tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue());
            initAndAcquire(queryRequest.getReadRateLimiter(), table, null);
            items = table.query(querySpec);
        }

        items.registerLowLevelResultListener(new LowLevelResultListener<QueryOutcome>() {

            private int totalProgress = 0;

            @Override
            public void onLowLevelResult(QueryOutcome queryOutcome) {
                DynamoRateLimiter dynamoRateLimiter = queryRequest.getReadRateLimiter();
                totalProgress += queryOutcome.getQueryResult().getItems().size();
                if (queryRequest.getProgressCallback() != null) {
                    queryRequest.getProgressCallback().reportProgress(totalProgress);
                }
                if (dynamoRateLimiter != null) {
                    dynamoRateLimiter.setConsumedCapacity(queryOutcome.getQueryResult().getConsumedCapacity());
                    dynamoRateLimiter.acquire();
                }
            }
        });
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(buildObjectFromDynamoItem(iterator.next(), tableDefinition, queryRequest.getResultClass(), null, queryRequest.getMigrationContext(), queryRequest.isWriteMigrationChange(), false, queryRequest.getSuffix()));
        }

        return results;
    }

    public <T extends DynamapRecordBean> ScanResult<T> scan(ScanRequest<T> scanRequest) {
        List<T> results = new ArrayList<>();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(scanRequest.getResultClass());
        Table table = getTable(tableDefinition.getTableName(prefix, scanRequest.getSuffix()));

        ScanSpec scanspec = new ScanSpec();
        if (scanRequest.getNames() != null) {
            scanspec.withNameMap(scanRequest.getNames());
        }
        if (scanRequest.getValues() != null) {
            scanspec.withValueMap(scanRequest.getValues());
        }
        if (scanRequest.getProjectionExpression() != null) {
            scanspec.withProjectionExpression(scanRequest.getProjectionExpression());
        }
        if (scanRequest.getFilterExpression() != null) {
            scanspec.withFilterExpression(scanRequest.getFilterExpression());
        }
        if (scanRequest.getStartExclusiveHashKey() != null) {
            scanspec.withExclusiveStartKey(scanRequest.getStartExclusiveHashKey(), scanRequest.getStartExclusiveRangeKey());
        }
        if (scanRequest.getMaxResultSize() != null) {
            scanspec.withMaxPageSize(scanRequest.getMaxResultSize());
        }

        if (scanRequest.getReadRateLimiter() != null) {
            if (scanRequest.getReadRateLimiter() != null) {
                scanRequest.getReadRateLimiter().init(table, scanRequest.getIndex() == null ? null : scanRequest.getIndex().getName());
                scanRequest.getReadRateLimiter().acquire();
            }
        }

        ItemCollection<ScanOutcome> scanItems;
        if (scanRequest.getIndex() != null) {
            scanItems = table.getIndex(scanRequest.getIndex().getName()).scan(scanspec);
        } else {
            scanItems = table.scan(scanspec);
        }

        scanItems.registerLowLevelResultListener(new LowLevelResultListener<ScanOutcome>() {
            private int totalProgress = 0;
            ProgressCallback progressCallback = scanRequest.getProgressCallback();

            @Override
            public void onLowLevelResult(ScanOutcome scanOutcome) {
                DynamoRateLimiter dynamoRateLimiter = scanRequest.getReadRateLimiter();
                totalProgress += scanOutcome.getScanResult().getItems().size();
                if (progressCallback != null) {
                    progressCallback.reportProgress(totalProgress);
                }
                if (dynamoRateLimiter != null) {
                    dynamoRateLimiter.setConsumedCapacity(scanOutcome.getScanResult().getConsumedCapacity());
                    dynamoRateLimiter.acquire();
                }
            }

        });

        Iterator<Item> iterator = scanItems.iterator();
        while (iterator.hasNext()) {
            results.add(buildObjectFromDynamoItem(iterator.next(), tableDefinition, scanRequest.getResultClass(), null, scanRequest.getMigrationContext(), scanRequest.isWriteMigrationChange(), scanRequest.getProjectionExpression() != null, scanRequest.getSuffix()));
        }

        String lastHashKey = null;
        Object lastRangeKey = null;
        if (scanItems.getLastLowLevelResult().getScanResult().getLastEvaluatedKey() != null) {
            Map<String, AttributeValue> lastEvaluated = scanItems.getLastLowLevelResult().getScanResult().getLastEvaluatedKey();
            lastHashKey = lastEvaluated.get(tableDefinition.getHashKey()).getS();
            if (tableDefinition.getRangeKey() != null) {
                lastRangeKey = lastEvaluated.get(tableDefinition.getRangeKey());
            }
        }
        return new ScanResult(lastHashKey, lastRangeKey, results, scanItems.getAccumulatedItemCount(), scanItems.getAccumulatedScannedCount());
    }

    public void save(SaveParams saveParams) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(saveParams.getDynamapRecordBean().getClass());
        putObject(saveParams.getDynamapRecordBean(), tableDefinition, !saveParams.isDisableOverwrite(),
                saveParams.isDisableOptimisticLocking(), saveParams.getWriteLimiter(), saveParams.getSuffix());
    }

    @Deprecated
    public void save(DynamapRecordBean object, DynamoRateLimiter writeLimiter) {
        SaveParams saveParams = new SaveParams(object).withWriteLimiter(writeLimiter);
        save(saveParams);
    }

    @Deprecated
    public void save(DynamapRecordBean object, boolean overwrite, DynamoRateLimiter writeLimiter) {
        SaveParams saveParams = new SaveParams(object).withDisableOverwrite(!overwrite).withWriteLimiter(writeLimiter);
        save(saveParams);
    }

    @Deprecated
    public void save(DynamapRecordBean object, boolean overwrite, boolean disableOptimisticLocking, DynamoRateLimiter writeLimiter) {
        SaveParams saveParams = new SaveParams(object).withDisableOverwrite(!overwrite)
                .withDisableOptimisticLocking(disableOptimisticLocking).withWriteLimiter(writeLimiter);
        save(saveParams);
    }

    @Deprecated
    public void save(DynamapRecordBean object, boolean overwrite, boolean disableOptimisticLocking, DynamoRateLimiter writeLimiter, String suffix) {
        SaveParams saveParams = new SaveParams(object).withDisableOverwrite(!overwrite)
                .withDisableOptimisticLocking(disableOptimisticLocking).withWriteLimiter(writeLimiter).withSuffix(suffix);
        save(saveParams);
    }

    public <U extends Updates<T>, T extends DynamapRecordBean<?>> T update(UpdateParams<T, U> updateParams) {
        Updates updates = updateParams.getUpdates();
        DynamoRateLimiter writeLimiter = updateParams.getWriteLimiter();
        String suffix = updateParams.getSuffix();

        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(updates.getTableName());
        UpdateItemSpec updateItemSpec = getUpdateItemSpec(updates, tableDefinition, updateParams.getDynamapReturnValue());
        Table table = getTable(tableDefinition.getTableName(prefix, suffix));

        logger.debug("About to submit DynamoDB Update: Update expression: {}, Conditional expression: {}, Values {}, Names: {}", updateItemSpec.getUpdateExpression(), updateItemSpec.getConditionExpression(), updateItemSpec.getValueMap(), updateItemSpec.getNameMap());
        try {
            if (writeLimiter != null) {
                writeLimiter.init(table);
                writeLimiter.acquire();
            }
            updateItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            if (logger.isDebugEnabled()) {
                logger.debug("UpdateItemOutcome: " + updateItemOutcome.getItem().toJSONPretty());
            }
            if (writeLimiter != null) {
                writeLimiter.setConsumedCapacity(updateItemOutcome.getUpdateItemResult().getConsumedCapacity());
            }

            if (updateParams.getDynamapReturnValue() == DynamapReturnValue.NONE) {
                return null;
            }
            if (updateParams.getDynamapReturnValue() == DynamapReturnValue.ALL_NEW ||
                    updateParams.getDynamapReturnValue() == DynamapReturnValue.ALL_OLD) {
                return (T) objectMapper.convertValue(updateItemOutcome.getItem().asMap(), updates.getTableClass());
            }

            // Merge updated values with existing
            try {
                Class beanClass = Class.forName(tableDefinition.getPackageName() + "." + tableDefinition.getType() + "Bean");
                Class interfaceClass = Class.forName(tableDefinition.getPackageName() + "." + tableDefinition.getType());
                Object updatesAsBean = beanClass.getDeclaredConstructor(interfaceClass).newInstance(updateParams.getUpdates());
                Map<String, Object> updatesAsMap = objectMapper.convertValue(updatesAsBean, new TypeReference<Map<String, Object>>() {
                });
                updatesAsMap.putAll(updateItemOutcome.getItem().asMap());
                return (T) objectMapper.convertValue(updatesAsMap, updates.getTableClass());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            String keyComponents = updateItemSpec.getKeyComponents().stream().map(Object::toString).collect(Collectors.joining(","));
            logger.debug("Error updating item: Key: " + keyComponents + " Update expression:" + updateItemSpec.getUpdateExpression() + " Conditional expression: " + updateItemSpec.getConditionExpression() + " Values: " + updateItemSpec.getValueMap() + " Names: " + updateItemSpec.getNameMap());
            throw e;
        }
    }

    @Deprecated
    public <U extends Updates, T extends DynamapRecordBean<?>> T update(U updates, DynamoRateLimiter writeLimiter) {
        return (T) update(new UpdateParams<>(updates).withWriteLimiter(writeLimiter));
    }

    @Deprecated
    public <U extends Updates, T extends DynamapRecordBean<?>> T update(U updates, DynamoRateLimiter writeLimiter, String suffix) {
        return (T) update(new UpdateParams<>(updates).withWriteLimiter(writeLimiter).withSuffix(suffix));
    }

    private static class GetItemInfo {
        public TableKeysAndAttributes keysAndAttributes;
        public TableDefinition tableDefinition;
        public GetObjectRequest getObjectRequest;
        public Table table;
    }

    private Multimap<String, Item> doBatchGetItem(Map<String, GetItemInfo> queryInfos, Map<String, ReadWriteRateLimiterPair> rateLimiters, int totalProgress, ProgressCallback progressCallback) {
        Multimap<String, Item> results = ArrayListMultimap.create();
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
                    setConsumedUnits(rateLimiters, consumedCapacity, false);
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
                initAndAcquire(rateLimiters, false);
                outcome = dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
            }
            totalProgress = totalProgress + tableItems.values().size();
            if (progressCallback != null) {
                if (!progressCallback.reportProgress(totalProgress)) {
                    return results;
                }
            }

        } while (unprocessedKeyCount > 0);

        return results;

    }

    private void setConsumedUnits(Map<String, ReadWriteRateLimiterPair> rateLimiters, ConsumedCapacity consumedCapacity, boolean write) {
        ReadWriteRateLimiterPair rateLimiterPair = rateLimiters.get(consumedCapacity.getTableName());
        if (rateLimiterPair != null) {
            DynamoRateLimiter rateLimiter = write ? rateLimiterPair.getWriteLimiter() : rateLimiterPair.getReadLimiter();
            if (rateLimiter != null) {
                rateLimiter.setConsumedCapacity(consumedCapacity);
            }
        }
    }

    private void initAndAcquire(Map<String, ReadWriteRateLimiterPair> rateLimiters, boolean write) {
        if (rateLimiters != null) {
            for (String tableName : rateLimiters.keySet()) {
                Table table = getTable(tableName);
                for (ReadWriteRateLimiterPair dynamoRateLimiters : rateLimiters.values()) {
                    DynamoRateLimiter rateLimiter = write ? dynamoRateLimiters.getWriteLimiter() : dynamoRateLimiters.getReadLimiter();
                    if (rateLimiter != null) {
                        rateLimiter.init(table);
                        rateLimiter.acquire();
                    }
                }
            }
        }
    }

    private void initAndAcquire(DynamoRateLimiter readRateLimiter, Table table, String indexName) {
        if (readRateLimiter != null) {
            readRateLimiter.init(table, indexName);
        }
    }

    private <T extends DynamapRecordBean> T buildObjectFromDynamoItem(Item item, TableDefinition tableDefinition, Class<T> resultClass, DynamoRateLimiter writeRateLimiter, Object migrationContext, boolean writeBack, boolean skipMigration) {
        return buildObjectFromDynamoItem(item, tableDefinition, resultClass, writeRateLimiter, migrationContext, writeBack, skipMigration, null);
    }

    private <T extends DynamapRecordBean> T buildObjectFromDynamoItem(Item item, TableDefinition tableDefinition, Class<T> resultClass, DynamoRateLimiter writeRateLimiter, Object migrationContext, boolean writeBack, boolean skipMigration, String suffix) {
        if (item == null) {
            return null;
        }

        T result;
        String schemaField = tableDefinition.getSchemaVersionField();
        int currentVersion = 0;
        if (tableDefinition.isEnableMigrations() && !skipMigration) {
            if (!item.hasAttribute(schemaField)) {
                Field field = tableDefinition.getField(tableDefinition.getHashKey());
                logger.warn("Schema version field does not exist for {} on item with hash key {}. Migrating item to current version", tableDefinition.getTableName(), item.get(field.getDynamoName()));
            } else {
                currentVersion = item.getInt(schemaField);
            }
        }

        if (tableDefinition.isEnableMigrations() && !skipMigration && currentVersion != tableDefinition.getVersion()) {
            List<Migration> migrations = schemaRegistry.getMigrations(resultClass);
            if (migrations != null) {
                for (Migration migration : migrations) {
                    if (migration.getVersion() > currentVersion) {
                        migration.migrate(item, currentVersion, migrationContext);
                    }
                }
                for (Migration migration : migrations) {
                    if (migration.getVersion() > currentVersion) {
                        migration.postMigration(item, currentVersion, migrationContext);
                    }
                }
            }
            item = item.withInt(schemaField, tableDefinition.getVersion());
            result = objectMapper.convertValue(item.asMap(), resultClass);
            if (writeBack) {
                putObject(result, tableDefinition, true, false, writeRateLimiter, suffix);
            }
        } else {
            result = objectMapper.convertValue(item.asMap(), resultClass);
        }

        return result;
    }

    private <T extends DynamapRecordBean> Item buildDynamoItemFromObject(T object, TableDefinition tableDefinition, boolean disableOptimisticLocking) {
        Map<String, Object> map = objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
        });
        Item item = new Item();
        if (tableDefinition.isEnableMigrations()) {
            item.withInt(tableDefinition.getSchemaVersionField(), tableDefinition.getVersion());
        }

        if (!disableOptimisticLocking && tableDefinition.isOptimisticLocking()) {
            int revision = (int) map.getOrDefault(Schema.REVISION_FIELD, 0);
            item.withInt(Schema.REVISION_FIELD, revision + 1);
        }

        Type type = tableDefinition.getTypes().stream().filter(t -> t.getName().equals(tableDefinition.getType())).findFirst().get();
        for (Field field : type.getPersistedFields()) {
            if (!map.containsKey(field.getDynamoName()) || map.get(field.getDynamoName()) == null) {
                continue;
            }
            // if field is a nested dynamap object then remove any non persisted fields
            if (field.isGeneratedType()) {
                Map<String, Object> objectToPersist = (Map<String, Object>) map.get(field.getDynamoName());
                Type fieldType = tableDefinition.getFieldType(field.getType());
                for (Field fieldToCheck : fieldType.getFields()) {
                    if (objectToPersist.containsKey(fieldToCheck.getName()) && !fieldToCheck.isPersist()) {
                        objectToPersist.remove(fieldToCheck.getName());
                    }
                }
            }
            // Jackson converts all collections to array lists, which in turn are treated as lists
            // Need to handle string sets specifically. String sets can also not be empty
            if (field.getMultiValue() == Field.MultiValue.SET && field.getType().equals("String")) {
                List list = (List) map.get(field.getDynamoName());
                if (list.size() > 0) {
                    item.with(field.getDynamoName(), new HashSet<>((List) map.get(field.getDynamoName())));
                }
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

    private <T extends DynamapRecordBean> void putObject(T object, TableDefinition tableDefinition, boolean overwrite, boolean disableOptimisticLocking, DynamoRateLimiter writeLimiter, String suffix) {

        Item item = buildDynamoItemFromObject(object, tableDefinition, disableOptimisticLocking);
        PutItemSpec putItemSpec = new PutItemSpec()
                .withItem(item)
                .withReturnValues(ReturnValue.NONE);
        String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
        ValueMap valueMap = new ValueMap();
        NameMap nameMap = new NameMap();
        List<String> conditionalExpressions = new ArrayList<>();
        if (!overwrite) {
            conditionalExpressions.add("attribute_not_exists(" + hashKeyFieldName + ")");
        }

        if (!disableOptimisticLocking && tableDefinition.isOptimisticLocking()) {
            // value is incremented in buildDynamoItemFromObject, so here i must get the original value
            int revision = item.getInt(Schema.REVISION_FIELD) - 1;
            if (revision > 0) {
                conditionalExpressions.add("#name0=:val0");
                nameMap.with("#name0", Schema.REVISION_FIELD);
                valueMap.withInt(":val0", revision);
            }
        }

        if (conditionalExpressions.size() > 0) {
            putItemSpec.withConditionExpression(String.join(" AND ", conditionalExpressions));
            if (valueMap.size() > 0) {
                putItemSpec.withNameMap(nameMap);
                putItemSpec.withValueMap(valueMap);
            }
        }

        Table table = getTable(tableDefinition.getTableName(prefix, suffix));
        try {
            if (writeLimiter != null) {
                writeLimiter.init(table);
                writeLimiter.acquire();
            }
            PutItemOutcome outcome = table.putItem(putItemSpec);
            if (writeLimiter != null) {
                writeLimiter.setConsumedCapacity(outcome.getPutItemResult().getConsumedCapacity());
            }
        } catch (Exception e) {
            logger.debug(getPutErrorMessage(putItemSpec));
            throw e;
        }
    }

    private String getPutErrorMessage(PutItemSpec putItemSpec) {
        return "Error putting item:" + putItemSpec.getItem().toJSON() + " Conditional expression: " + putItemSpec.getConditionExpression() + " Values: " + putItemSpec.getValueMap() + " Names: " + putItemSpec.getNameMap();
    }

    private boolean hasAttributeDefinition(Collection<AttributeDefinition> attributeDefinitions, String name) {
        return attributeDefinitions.stream().anyMatch(d -> d.getAttributeName().equals(name));
    }

    private UpdateItemSpec getUpdateItemSpec(Updates updates, TableDefinition tableDefinition, DynamapReturnValue returnValue) {
        DynamoExpressionBuilder expressionBuilder = updates.getExpressionBuilder();
        expressionBuilder.setObjectMapper(objectMapper);
        updates.processUpdateExpression();

        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withReturnValues(ReturnValue.fromValue(returnValue.toString()));
        Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
        if (updates.getRangeKeyValue() != null) {
            Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
            updateItemSpec.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue(), rangeField.getDynamoName(), updates.getRangeKeyValue());
        } else {
            updateItemSpec.withPrimaryKey(hashField.getDynamoName(), updates.getHashKeyValue());
        }
        String conditionalExpression = expressionBuilder.buildConditionalExpression();
        if (null != conditionalExpression && !"".equals(conditionalExpression)) {
            updateItemSpec = updateItemSpec.withConditionExpression(conditionalExpression);
        }
        String updateExpression = expressionBuilder.buildUpdateExpression();
        if (null != updateExpression && !"".equals(updateExpression)) {
            updateItemSpec = updateItemSpec.withUpdateExpression(updateExpression);
        }
        if (!expressionBuilder.getNameMap().isEmpty()) {
            updateItemSpec = updateItemSpec.withNameMap(expressionBuilder.getNameMap());
        }
        if (!expressionBuilder.getValueMap().isEmpty()) {
            updateItemSpec = updateItemSpec.withValueMap(expressionBuilder.getValueMap());
        }

        return updateItemSpec;
    }


    public void delete(DeleteRequest deleteRequest) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(deleteRequest.getResultClass());
        Table table = getTable(tableDefinition.getTableName(prefix, deleteRequest.getSuffix()));

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec();
        Field hashField = tableDefinition.getField(tableDefinition.getHashKey());
        if (deleteRequest.getRangeKeyValue() != null) {
            Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
            deleteItemSpec.withPrimaryKey(hashField.getDynamoName(), deleteRequest.getHashKeyValue(), rangeField.getDynamoName(), deleteRequest.getRangeKeyValue());
        } else {
            deleteItemSpec.withPrimaryKey(hashField.getDynamoName(), deleteRequest.getHashKeyValue());
        }
        if (deleteRequest.getConditionExpression() != null) {
            deleteItemSpec.withConditionExpression(deleteRequest.getConditionExpression());
            if (deleteRequest.getNames() != null) {
                deleteItemSpec.withNameMap(deleteRequest.getNames());
            }
            if (deleteRequest.getValues() != null) {
                deleteItemSpec.withValueMap(deleteRequest.getValues());
            }
        }
        table.deleteItem(deleteItemSpec);
    }

    public void batchDelete(BatchDeleteRequest batchDeleteRequest) {

        List<List<DeleteRequest>> partitions = Lists.partition(batchDeleteRequest.getDeleteRequests(), MAX_BATCH_SIZE);
        for (List<DeleteRequest> deleteRequests : partitions) {

            Map<String, TableWriteItems> tableWriteItems = new HashMap<>();
            for (DeleteRequest deleteRequest : deleteRequests) {
                TableDefinition tableDefinition = schemaRegistry.getTableDefinition(deleteRequest.getResultClass());
                Field hashField = tableDefinition.getField(tableDefinition.getHashKey());

                String tableName = tableDefinition.getTableName(prefix, deleteRequest.getSuffix());
                TableWriteItems writeItems = tableWriteItems.get(tableName);
                if (writeItems == null) {
                    writeItems = new TableWriteItems(tableName);
                    tableWriteItems.put(tableName, writeItems);
                }

                if (tableDefinition.getRangeKey() != null) {
                    Field rangeField = tableDefinition.getField(tableDefinition.getRangeKey());
                    writeItems.addHashAndRangePrimaryKeysToDelete(hashField.getDynamoName(), rangeField.getDynamoName(),
                            deleteRequest.getHashKeyValue(), deleteRequest.getRangeKeyValue());
                } else {
                    writeItems.addHashOnlyPrimaryKeysToDelete(hashField.getDynamoName(), deleteRequest.getHashKeyValue());
                }
            }
            doBatchWriteItem(batchDeleteRequest.getRateLimiters(), tableWriteItems);
        }
    }


    public <T extends DynamapRecordBean> void batchSave(List<T> objects, Map<Class, DynamoRateLimiter> writeLimiterMap) {
        batchSave(objects, writeLimiterMap, null);
    }

    public <T extends DynamapRecordBean> void batchSave(List<T> objects, Map<Class, DynamoRateLimiter> writeLimiterMap, String suffix) {
        final List<List<T>> objectsBatch = Lists.partition(objects, MAX_BATCH_SIZE);
        for (List<T> batch : objectsBatch) {
            logger.debug("Sending batch to save of size: {}", batch.size());
            Map<String, TableWriteItems> tableWriteItems = new HashMap<>();

            for (DynamapRecordBean object : batch) {
                TableDefinition tableDefinition = schemaRegistry.getTableDefinition(object.getClass());
                Item item = buildDynamoItemFromObject(object, tableDefinition);

                String tableName = tableDefinition.getTableName(prefix, suffix);
                TableWriteItems writeItems = tableWriteItems.getOrDefault(tableName, new TableWriteItems(tableName));
                tableWriteItems.put(tableName, writeItems.addItemToPut(item));
            }

            doBatchWriteItem(writeLimiterMap, tableWriteItems);
        }
    }

    private void doBatchWriteItem(Map<Class, DynamoRateLimiter> writeLimiterMap, Map<String, TableWriteItems> tableWriteItems) {

        Map<String, DynamoRateLimiter> writeLimiterMapByTable = null;
        if (writeLimiterMap != null) {
            writeLimiterMapByTable = new HashMap<>();
            for (Map.Entry<Class, DynamoRateLimiter> entry : writeLimiterMap.entrySet()) {
                writeLimiterMapByTable.put(schemaRegistry.getTableDefinition(entry.getKey()).getTableName(prefix), entry.getValue());
            }
        }

        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec()
                .withTableWriteItems(tableWriteItems.values().toArray(new TableWriteItems[tableWriteItems.size()]));

        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(batchWriteItemSpec);

        while (outcome.getUnprocessedItems().size() > 0) {
            if (writeLimiterMapByTable != null && outcome.getBatchWriteItemResult().getConsumedCapacity() != null) {
                // need better testing
                for (ConsumedCapacity consumedCapacity : outcome.getBatchWriteItemResult().getConsumedCapacity()) {
                    DynamoRateLimiter rateLimiter = writeLimiterMapByTable.get(consumedCapacity.getTableName());
                    rateLimiter.setConsumedCapacity(consumedCapacity);
                    logger.debug("Set rate limiter capacity {}", consumedCapacity.getCapacityUnits());
                    Table table = getTable(consumedCapacity.getTableName());
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

    private Table getTable(String tableName) {
        Table table = tableCache.get(tableName);
        if (table == null) {
            table = dynamoDB.getTable(tableName);
            tableCache.put(tableName, table);
        }
        return table;
    }
}
