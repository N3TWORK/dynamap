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
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.n3twork.BatchSaveParams;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.TableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Dynamap {

    private static final Logger logger = LoggerFactory.getLogger(Dynamap.class);

    private final AmazonDynamoDB amazonDynamoDB;
    private final DynamoDB dynamoDB;
    private final SchemaRegistry schemaRegistry;
    private final TableCache tableCache;
    private String prefix;
    private ObjectMapper objectMapper;
    private Map<String, CreateTableRequest> createTableRequests = new HashMap<>();
    private WriteOpFactory writeOpFactory;
    private ReadOpFactory readOpFactory;
    private DynamapBeanFactory dynamapBeanFactory;

    private static final int MAX_BATCH_SIZE = 25;
    private static final int MAX_BATCH_GET_SIZE = 100;

    public Dynamap(AmazonDynamoDB amazonDynamoDB, SchemaRegistry schemaRegistry) {
        this.amazonDynamoDB = amazonDynamoDB;
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
        this.tableCache = new TableCache(this.dynamoDB);
        this.schemaRegistry = schemaRegistry;
        this.objectMapper = new ObjectMapper();
        this.writeOpFactory = new WriteOpFactory(objectMapper, this.prefix, schemaRegistry);
        this.readOpFactory = new ReadOpFactory(schemaRegistry, this.prefix);
        this.dynamapBeanFactory = new DynamapBeanFactory(schemaRegistry, this.objectMapper);
    }

    public Dynamap withObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.dynamapBeanFactory = new DynamapBeanFactory(schemaRegistry, this.objectMapper);
        return this;
    }

    public Dynamap withPrefix(String prefix) {
        this.prefix = prefix;
        this.writeOpFactory = new WriteOpFactory(objectMapper, this.prefix, schemaRegistry);
        this.readOpFactory = new ReadOpFactory(schemaRegistry, this.prefix);
        return this;
    }

    public SchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }

    public void createTables(boolean deleteIfExists) {
        createTables(deleteIfExists, 1, 1, request -> {});
    }

    public void createTables(boolean deleteIfExists, Consumer<CreateTableRequest> requestTransformer) {
        createTables(deleteIfExists, 1, 1, requestTransformer);
    }

    public void createTables(boolean deleteIfExists, long readProvisioning, long writeProvisioning) {
        createTables(deleteIfExists, readProvisioning, writeProvisioning, request -> {});
    }

    public void createTables(boolean deleteIfExists, long readProvisioning, long writeProvisioning, Consumer<CreateTableRequest> requestTransformer) {
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
                attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getElementType().equals("String") ? "S" : "N"));
                keySchema.add(new KeySchemaElement().withAttributeName(field.getDynamoName()).withKeyType(KeyType.RANGE));
            }

            if (tableDefinition.getGlobalSecondaryIndexes() != null) {
                for (com.n3twork.dynamap.model.Index index : tableDefinition.getGlobalSecondaryIndexes()) {
                    Projection projection = new Projection().withProjectionType(index.getProjectionType());
                    GlobalSecondaryIndex gsi = new GlobalSecondaryIndex()
                            .withIndexName(index.getIndexName())
                            .withProvisionedThroughput(new ProvisionedThroughput()
                                    .withReadCapacityUnits(1L)
                                    .withWriteCapacityUnits(1L))
                            .withProjection(projection);
                    ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<>();
                    Field field = tableDefinition.getField(index.getHashKey());
                    indexKeySchema.add(new KeySchemaElement()
                            .withAttributeName(field.getDynamoName())
                            .withKeyType(KeyType.HASH));
                    if (!hasAttributeDefinition(attributeDefinitions, field.getDynamoName())) {
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getElementType().equals("String") ? "S" : "N"));
                    }

                    if (index.getRangeKey() != null) {
                        Field rangeField = tableDefinition.getField(index.getRangeKey());
                        indexKeySchema.add(new KeySchemaElement()
                                .withAttributeName(rangeField.getDynamoName())
                                .withKeyType(KeyType.RANGE));

                        if (!hasAttributeDefinition(attributeDefinitions, rangeField.getDynamoName())) {
                            attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeField.getDynamoName()).withAttributeType(rangeField.getElementType().equals("String") ? "S" : "N"));
                        }
                    }
                    gsi.setKeySchema(indexKeySchema);
                    if (index.getNonKeyFields() != null) {
                        List<String> nonKeyAttributes = new ArrayList<>();
                        for (String nonKeyField : index.getNonKeyFields()) {
                            Field nkf = tableDefinition.getField(nonKeyField);
                            nonKeyAttributes.add(nkf.getDynamoName());
                        }
                        projection.withNonKeyAttributes(nonKeyAttributes);
                    }
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
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(field.getDynamoName()).withAttributeType(field.getElementType().equals("String") ? "S" : "N"));
                    }

                    Field rangeField = tableDefinition.getField(index.getRangeKey());
                    indexKeySchema.add(new KeySchemaElement()
                            .withAttributeName(rangeField.getDynamoName())
                            .withKeyType(KeyType.RANGE));

                    if (!hasAttributeDefinition(attributeDefinitions, rangeField.getDynamoName())) {
                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(rangeField.getDynamoName()).withAttributeType(rangeField.getElementType().equals("String") ? "S" : "N"));
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
                            .withReadCapacityUnits(readProvisioning)
                            .withWriteCapacityUnits(writeProvisioning));

            createTableRequests.put(tableDefinition.getTableName(), request);

            if (globalSecondaryIndexes.size() > 0) {
                request = request.withGlobalSecondaryIndexes(globalSecondaryIndexes);
            }

            if (localSecondaryIndexes.size() > 0) {
                request = request.withLocalSecondaryIndexes(localSecondaryIndexes);
            }

            requestTransformer.accept(request);

            if (deleteIfExists) {
                TableUtils.deleteTableIfExists(amazonDynamoDB, new DeleteTableRequest().withTableName(tableDefinition.getTableName(prefix)));
            }
            boolean wasCreated = TableUtils.createTableIfNotExists(amazonDynamoDB, request);
            updateTableTtl(tableDefinition, Optional.empty());
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
        boolean wasCreated = TableUtils.createTableIfNotExists(amazonDynamoDB, createTableRequest);
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(baseTableName);
        updateTableTtl(tableDefinition, Optional.of(newTableName));
        return wasCreated;
    }

    /**
     * Set a table's TTL field in DynamoDB.
     * <p>
     * Does nothing if:
     * - the provided TableDefinition lacks a TTL Field.
     * - the table already has the desired TTL ENABLED.
     * - the table has another field ENABLED as TTL. DynamoDB requires that you first disable the current TTL and then enable a new TTL.
     * - the table's TimeToLiveStatus is ENABLING or DISABLING.
     * <p>
     * Changing the TTL on a table in DynamoDB is an asynchronous process and can take a while to apply.
     * Documents with a TTL attribute set are expired by DynamoDB using a background process and it is far
     * from exact: items may linger beyond their TTL and application code should account for this.
     * <p>
     * Read the docs for further details: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html
     *
     * @param tableDefinition   The table to update. Should have a TTL Field defined otherwise this call will no-op.
     * @param tableNameOverride Optional table name override to support current test patterns.
     */
    private void updateTableTtl(TableDefinition tableDefinition, Optional<String> tableNameOverride) {
        Optional<Field> ttlField = tableDefinition.getTtlField();
        if (!ttlField.isPresent()) {
            return;
        }
        // Describe current TTL settings for the table.
        String tableName = tableNameOverride.isPresent() ? tableNameOverride.get() : tableDefinition.getTableName(prefix);
        TimeToLiveDescription timeToLiveDescription =
                amazonDynamoDB.describeTimeToLive(new DescribeTimeToLiveRequest().withTableName(tableName)).getTimeToLiveDescription();
        TimeToLiveStatus timeToLiveStatus = TimeToLiveStatus.fromValue(timeToLiveDescription.getTimeToLiveStatus());

        logger.debug("TimeToLiveDescription for table {} is {}", tableName, timeToLiveDescription);
        switch (timeToLiveStatus) {
            case DISABLED: {
                // No current TTL is set on the table, we're clear to apply ours.
                logger.info("Setting TTL for table {} and field {}.", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName());
                updateTimeToLive(tableDefinition, tableNameOverride, ttlField);
                break;
            }
            case ENABLED:
                if (ttlField.get().getDynamoName().equals(timeToLiveDescription.getAttributeName())) {
                    // TTL is already enabled on the correct field. Nothing to do.
                    logger.info("TTL for table {} is set to field {}.", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName());
                } else {
                    // TTL is ENABLED but not on the desired field.
                    logger.warn("Failed to set TTL for table {} and field {}. Table already has TTL field {}. You must disable this TTL field before choosing a new one.", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName(), timeToLiveStatus, timeToLiveDescription.getAttributeName());
                }
                break;
            case ENABLING:
                if (ttlField.get().getDynamoName().equals(timeToLiveDescription.getAttributeName())) {
                    // TTL is enabling on the correct field. Nothing to do but wait for DynamoDB.
                    logger.info("TTL for table {} and field {} is ENABLING.", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName());
                } else {
                    logger.warn("Failed to set TTL for table {} and field {}. TimeToLiveStatus is currently {} on field {}", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName(), timeToLiveStatus, timeToLiveDescription.getAttributeName());
                }
                break;
            case DISABLING:
                logger.warn("Failed to set TTL for table {} and field {}. TimeToLiveStatus is currently {} on field {}", tableDefinition.getTableName(prefix), ttlField.get().getDynamoName(), timeToLiveStatus, timeToLiveDescription.getAttributeName());
                break;
        }
    }

    // Make the DynamoDB call, handle errors.
    private void updateTimeToLive(TableDefinition tableDefinition, Optional<String> tableNameOverride, Optional<Field> ttlField) {
        UpdateTimeToLiveRequest updateTimeToLiveRequest = new UpdateTimeToLiveRequest()
                .withTableName(tableNameOverride.isPresent() ? tableNameOverride.get() : tableDefinition.getTableName(prefix))
                .withTimeToLiveSpecification(new TimeToLiveSpecification().withAttributeName(ttlField.get().getDynamoName()).withEnabled(true));
        try {
            amazonDynamoDB.updateTimeToLive(updateTimeToLiveRequest);
        } catch (ResourceInUseException e) {
            // This will happen if another request kicks off a TTL change between our DescribeTimeToLiveRequest
            // and UpdateTimeToLiveRequest calls.
        }
    }

    public <T extends DynamapRecordBean> T getObject(GetObjectParams<T> getObjectParams) {
        BatchGetObjectParams<T> batchGetObjectParams = new BatchGetObjectParams<T>()
                .withGetObjectRequests(Arrays.asList(getObjectParams.getGetObjectRequest()))
                .withMigrationContext(getObjectParams.getMigrationContext())
                .withWriteMigrationChange(getObjectParams.isWriteMigrationChange());
        if (getObjectParams.getRateLimiters() != null) {
            batchGetObjectParams.withRateLimiters(ImmutableMap.of(getObjectParams.getGetObjectRequest().getResultClass(), getObjectParams.getRateLimiters()));
        }
        Map<Class, List<Object>> results = batchGetObject(batchGetObjectParams);
        List<Object> resultList = results.values().iterator().next();
        if (resultList.size() > 0) {
            return (T) resultList.get(0);
        }
        return null;
    }

    public Map<Class, List<Object>> batchGetObject(BatchGetObjectParams batchGetObjectParams) {
        Map<String, ReadWriteRateLimiterPair> rateLimitersByTable = new HashMap<>();
        Map<Class, ReadWriteRateLimiterPair> rateLimiters = batchGetObjectParams.getRateLimiters();
        if (batchGetObjectParams.getRateLimiters() != null) {
            for (Class resultClass : rateLimiters.keySet()) {
                ReadWriteRateLimiterPair rateLimiterPair = rateLimiters.get(resultClass);
                if (rateLimiterPair == null) {
                    continue;
                }
                rateLimitersByTable.put(schemaRegistry.getTableDefinition(resultClass).getTableName(prefix), rateLimiterPair);
            }
        }

        List<List<GetObjectRequest>> partitions = Lists.partition(new ArrayList<>(batchGetObjectParams.getGetObjectRequests()), MAX_BATCH_GET_SIZE);
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
                getItemInfo.table = tableCache.getTable(tableName);
            }

            Multimap<String, Item> allItems = doBatchGetItem(queryInfos, rateLimitersByTable, totalProgress, batchGetObjectParams.getProgressCallback());
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
                    DynamapLoadService dynamapBeanLoader = new DynamapLoadService(schemaRegistry, dynamapBeanFactory, objectMapper, prefix, tableCache)
                            .withWriteLimiter(writeLimiter)
                            .writeBack(batchGetObjectParams.isWriteMigrationChange())
                            .withMigrationContext(batchGetObjectParams.getMigrationContext())
                            .withSuffix(getItemInfo.getObjectRequest.getSuffix());
                    resultsForClass.add(dynamapBeanLoader.loadItem(item, getItemInfo.getObjectRequest.getResultClass()));
                }
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public <T extends DynamapRecordBean> List<T> batchGetObjectSingleCollection(BatchGetObjectParams<T> batchGetObjectParams) {
        Collection<GetObjectRequest<T>> getObjectRequests = batchGetObjectParams.getGetObjectRequests();
        if (getObjectRequests.size() == 0) {
            return Collections.emptyList();
        }
        // ensure that only one result class has been specified
        GetObjectRequest<T> getObjectRequest = getObjectRequests.iterator().next();
        String resultClass = getObjectRequest.getResultClass().getCanonicalName();
        if (getObjectRequests.stream().anyMatch(r -> !r.getResultClass().getCanonicalName().equals(resultClass))) {
            throw new IllegalArgumentException("More than one ResultClass has been specified");
        }
        if (batchGetObjectParams.getReadWriteRateLimiterPair() != null) {
            batchGetObjectParams.withRateLimiters(ImmutableMap.of(getObjectRequest.getResultClass(), batchGetObjectParams.getReadWriteRateLimiterPair()));
        }
        List<T> result = (List<T>) (Object) batchGetObject(batchGetObjectParams).get(getObjectRequest.getResultClass());
        return result == null ? Collections.emptyList() : result;
    }

    public <T extends DynamapRecordBean> List<T> query(QueryRequest<T> queryRequest) {
        return queryResult(queryRequest).getResults();
    }

    public <T extends DynamapRecordBean> QueryResult<T> queryResult(QueryRequest<T> queryRequest) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(queryRequest.getResultClass());
        Table table = tableCache.getTable(tableDefinition.getTableName(prefix, queryRequest.getSuffix()));
        QuerySpec querySpec = new QuerySpec()
                .withConsistentRead(queryRequest.isConsistentRead())
                .withKeyConditionExpression(queryRequest.getKeyConditionExpression())
                .withFilterExpression(queryRequest.getFilterExpression())
                .withProjectionExpression(queryRequest.getProjectionExpression())
                .withNameMap(queryRequest.getNames())
                .withValueMap(queryRequest.getValues())
                .withScanIndexForward(queryRequest.isScanIndexForward())
                .withMaxResultSize(queryRequest.getMaxResultSize())
                .withMaxPageSize(queryRequest.getMaxPageSize())
                .withExclusiveStartKey(queryRequest.getExclusiveStartKeys());

        if (queryRequest.getKeyConditionExpression() == null) {
            querySpec.withHashKey(tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue())
                    .withRangeKeyCondition(queryRequest.getRangeKeyCondition());
        }

        QueryFilter[] queryFilters = queryRequest.getQueryFilters();
        if (queryRequest.getFilterExpression() == null && queryFilters.length > 0) {
            querySpec.withQueryFilters(queryFilters);
        }

        Select select = queryRequest.getSelect();
        if (select != null) {
            querySpec.withSelect(select);
        }

        final ItemCollection<QueryOutcome> items;
        if (queryRequest.getIndex() != null) {
            com.n3twork.dynamap.model.Index indexDef = null;
            if (tableDefinition.getGlobalSecondaryIndexes() != null) {
                indexDef = tableDefinition.getGlobalSecondaryIndexes().stream().filter(i -> i.getIndexName().equals(queryRequest.getIndex().getName())).findFirst().get();
            } else if (tableDefinition.getLocalSecondaryIndexes() != null) {
                indexDef = tableDefinition.getLocalSecondaryIndexes().stream().filter(i -> i.getIndexName().equals(queryRequest.getIndex().getName())).findFirst().get();
            }
            String indexName = indexDef.getIndexName();
            Index index = table.getIndex(indexDef.getIndexName());
            if (queryRequest.getKeyConditionExpression() == null) {
                querySpec.withHashKey(tableDefinition.getField(indexDef.getHashKey()).getDynamoName(), queryRequest.getHashKeyValue());
            }
            initRateLimiter(queryRequest.getReadRateLimiter(), table, indexName);
            items = index.query(querySpec);
        } else {
            initRateLimiter(queryRequest.getReadRateLimiter(), table, null);
            items = table.query(querySpec);
        }

        items.registerLowLevelResultListener(new LowLevelResultListener<QueryOutcome>() {

            private int totalProgress = 0;

            @Override
            public void onLowLevelResult(QueryOutcome queryOutcome) {
                DynamoRateLimiter dynamoRateLimiter = queryRequest.getReadRateLimiter();
                totalProgress += queryOutcome.getQueryResult().getCount();
                if (queryRequest.getProgressCallback() != null) {
                    queryRequest.getProgressCallback().reportProgress(totalProgress);
                }
                if (dynamoRateLimiter != null) {
                    dynamoRateLimiter.setConsumedCapacity(queryOutcome.getQueryResult().getConsumedCapacity());
                    dynamoRateLimiter.acquire();
                }
            }
        });

        ItemIterator<T> itemIterator = new ItemIterator<T>(items) {

            @Override
            public T next() {
                DynamapLoadService dynamapBeanLoader = new DynamapLoadService(schemaRegistry, dynamapBeanFactory, objectMapper, prefix, tableCache)
                        .skipMigration(queryRequest.getProjectionExpression() != null)
                        .writeBack(queryRequest.isWriteMigrationChange())
                        .withMigrationContext(queryRequest.getMigrationContext())
                        .withSuffix(queryRequest.getSuffix());
                return dynamapBeanLoader.loadItem(iterator.next(), queryRequest.getResultClass());
            }

            @Override
            protected Map<String, AttributeValue> getLowLevelLastEvaluatedKey() {
                return items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey();
            }
        };


        return new QueryResult<>(itemIterator);
    }


    public <T extends DynamapRecordBean> ScanResult<T> scan(ScanRequest<T> scanRequest) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(scanRequest.getResultClass());
        Table table = tableCache.getTable(tableDefinition.getTableName(prefix, scanRequest.getSuffix()));

        ScanSpec scanspec = new ScanSpec();
        if (scanRequest.getNames() != null) {
            scanspec.withNameMap(scanRequest.getNames());
        }
        if (scanRequest.getValues() != null) {
            scanspec.withValueMap(scanRequest.getValues());
        }
        if (scanRequest.getSelect() != null) {
            scanspec.withSelect(scanRequest.getSelect());
        }
        if (scanRequest.getProjectionExpression() != null) {
            scanspec.withProjectionExpression(scanRequest.getProjectionExpression());
        }
        if (scanRequest.getFilterExpression() != null) {
            scanspec.withFilterExpression(scanRequest.getFilterExpression());
        }
        if (scanRequest.getExclusiveStartKeys() != null) {
            scanspec.withExclusiveStartKey(scanRequest.getExclusiveStartKeys());
        }

        if (scanRequest.getTotalSegments() != null && scanRequest.getSegment() != null) {
            scanspec.withSegment(scanRequest.getSegment());
            scanspec.withTotalSegments(scanRequest.getTotalSegments());
        }

        if (scanRequest.getMaxResultSize() != null) {
            scanspec.withMaxResultSize(scanRequest.getMaxResultSize());
        }
        if (scanRequest.getMaxPageSize() != null) {
            scanspec.withMaxPageSize(scanRequest.getMaxPageSize());
        }

        if (scanRequest.getReadRateLimiter() != null) {
            if (scanRequest.getReadRateLimiter() != null) {
                scanRequest.getReadRateLimiter().init(table, scanRequest.getIndex() == null ? null : scanRequest.getIndex().getName());
                scanRequest.getReadRateLimiter().acquire();
            }
        }

        final ItemCollection<ScanOutcome> scanItems;
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
                totalProgress += scanOutcome.getScanResult().getCount();
                if (progressCallback != null) {
                    progressCallback.reportProgress(totalProgress);
                }
                if (dynamoRateLimiter != null) {
                    dynamoRateLimiter.setConsumedCapacity(scanOutcome.getScanResult().getConsumedCapacity());
                    dynamoRateLimiter.acquire();
                }
            }

        });


        ItemIterator<T> itemIterator = new ItemIterator<T>(scanItems) {
            @Override
            public T next() {
                DynamapLoadService dynamapBeanLoader = new DynamapLoadService(schemaRegistry, dynamapBeanFactory, objectMapper, prefix, tableCache)
                        .skipMigration(scanRequest.getProjectionExpression() != null)
                        .writeBack(scanRequest.isWriteMigrationChange())
                        .withMigrationContext(scanRequest.getMigrationContext())
                        .withSuffix(scanRequest.getSuffix());
                return dynamapBeanLoader.loadItem(iterator.next(), scanRequest.getResultClass());
            }

            @Override
            protected Map<String, AttributeValue> getLowLevelLastEvaluatedKey() {
                return scanItems.getLastLowLevelResult().getScanResult().getLastEvaluatedKey();
            }
        };

        return new ScanResult<>(itemIterator);
    }

    public void save(SaveParams saveParams) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(saveParams.getDynamapRecordBean().getClass());
        new DynamapSaveService(objectMapper, prefix, tableCache)
                .saveBean(saveParams.getDynamapRecordBean(),
                        tableDefinition,
                        !saveParams.isDisableOverwrite(),
                        saveParams.isDisableOptimisticLocking(),
                        false,
                        saveParams.getWriteLimiter(),
                        saveParams.getSuffix(),
                        saveParams.getConditionExpressions(),
                        saveParams.getNames(),
                        saveParams.getValues());
    }

    public <T extends DynamapPersisted<U>, U extends RecordUpdates<T>, R extends UpdateResult<T, U>> R update(UpdateParams<T> updateParams) {
        RecordUpdates<T> updates = updateParams.getUpdates();
        DynamoRateLimiter writeLimiter = updateParams.getWriteLimiter();
        String suffix = updateParams.getSuffix();

        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(updates.getTableName());
        UpdateItemSpec updateItemSpec = getUpdateItemSpec(updates, tableDefinition, updateParams.getDynamapReturnValue());
        Table table = tableCache.getTable(tableDefinition.getTableName(prefix, suffix));

        logger.debug("About to submit DynamoDB Update: Update expression: {}, Conditional expression: {}, Values {}, Names: {}", updateItemSpec.getUpdateExpression(), updateItemSpec.getConditionExpression(), updateItemSpec.getValueMap(), updateItemSpec.getNameMap());
        try {
            if (writeLimiter != null) {
                writeLimiter.init(table);
                writeLimiter.acquire();
                updateItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            }

            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);

            Class updateResultClass = Class.forName(tableDefinition.getPackageName() + "." + tableDefinition.getType() + "UpdateResultBean");
            Constructor<R> constructor = updateResultClass.getConstructors()[0];

            if (updateParams.getDynamapReturnValue() == DynamapReturnValue.UPDATED_NEW && updateItemOutcome.getItem() == null) {
                // nothing changed
                return constructor.newInstance(updates, null);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("UpdateItemOutcome: " + updateItemOutcome.getItem().toJSONPretty());
            }
            if (writeLimiter != null) {
                writeLimiter.setConsumedCapacity(updateItemOutcome.getUpdateItemResult().getConsumedCapacity());
            }

            if (updateParams.getDynamapReturnValue() == DynamapReturnValue.NONE) {
                return null;
            }

            Class beanClass = Class.forName(tableDefinition.getPackageName() + "." + tableDefinition.getType() + "Bean");
            T bean = (T) dynamapBeanFactory.asDynamapBean(updateItemOutcome.getItem(), beanClass);
            return constructor.newInstance(updates, bean);


        } catch (ClassNotFoundException e) {
            logger.error("Cannot find bean class " + tableDefinition.getPackageName() + "." + tableDefinition.getType() + "Bean");
            throw new RuntimeException(e);
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
            logger.error("Cannot instantiate " + tableDefinition.getPackageName() + "." + tableDefinition.getType() + "UpdateResult");
            throw new RuntimeException(e);
        } catch (Exception e) {
            String keyComponents = updateItemSpec.getKeyComponents().stream().map(Object::toString).collect(Collectors.joining(","));
            logger.debug("Error updating item: Key: " + keyComponents + " Update expression:" + updateItemSpec.getUpdateExpression() + " Conditional expression: " + updateItemSpec.getConditionExpression() + " Values: " + updateItemSpec.getValueMap() + " Names: " + updateItemSpec.getNameMap());
            throw e;
        }
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

        initRateLimiterAndAcquire(rateLimiters, false);
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
                totalProgress += items.size();
                results.putAll(tableName, items);
            }
            // Check for unprocessed keys which could happen if it exceeds provisioned
            // throughput or reach the limit on response size.
            Map<String, KeysAndAttributes> unprocessedKeys = outcome.getUnprocessedKeys();

            unprocessedKeyCount = unprocessedKeys.size();
            if (unprocessedKeyCount != 0) {
                initRateLimiterAndAcquire(rateLimiters, false);
                if (rateLimiters != null) {
                    outcome = dynamoDB.batchGetItemUnprocessed(ReturnConsumedCapacity.TOTAL, unprocessedKeys);
                } else {
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
                }
            }

            if (progressCallback != null) {
                if (!progressCallback.reportProgress(totalProgress)) {
                    return results;
                }
            }

        } while (unprocessedKeyCount > 0);

        return results;

    }

    private void setConsumedUnits(Map<String, ReadWriteRateLimiterPair> rateLimiters, ConsumedCapacity consumedCapacity, boolean write) {
        if (rateLimiters == null) {
            return;
        }

        ReadWriteRateLimiterPair rateLimiterPair = rateLimiters.get(consumedCapacity.getTableName());
        if (rateLimiterPair != null) {
            DynamoRateLimiter rateLimiter = write ? rateLimiterPair.getWriteLimiter() : rateLimiterPair.getReadLimiter();
            if (rateLimiter != null) {
                rateLimiter.setConsumedCapacity(consumedCapacity);
            }
        }
    }

    private void initRateLimiterAndAcquire(Map<String, ReadWriteRateLimiterPair> rateLimiters, boolean write) {
        if (rateLimiters != null) {
            for (String tableName : rateLimiters.keySet()) {
                Table table = tableCache.getTable(tableName);
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

    private void initRateLimiter(DynamoRateLimiter readRateLimiter, Table table, String indexName) {
        if (readRateLimiter != null) {
            readRateLimiter.init(table, indexName);
        }
    }


    private boolean hasAttributeDefinition(Collection<AttributeDefinition> attributeDefinitions, String name) {
        return attributeDefinitions.stream().anyMatch(d -> d.getAttributeName().equals(name));
    }

    private UpdateItemSpec getUpdateItemSpec(RecordUpdates updates, TableDefinition tableDefinition, DynamapReturnValue returnValue) {
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
        Table table = tableCache.getTable(tableDefinition.getTableName(prefix, deleteRequest.getSuffix()));

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

    public <T extends DynamapRecordBean> void batchSave(BatchSaveParams<T> batchSaveParams) {
        final List<List<T>> objectsBatch = Lists.partition(batchSaveParams.getDynamapRecordBeans(), MAX_BATCH_SIZE);

        Map<String, DynamoRateLimiter> writeLimiterMapByTable = null;
        if (batchSaveParams.getWriteLimiters() != null) {
            writeLimiterMapByTable = new HashMap<>();
            for (Map.Entry<Class, DynamoRateLimiter> entry : batchSaveParams.getWriteLimiters().entrySet()) {
                String tableName = schemaRegistry.getTableDefinition(entry.getKey()).getTableName(prefix);
                DynamoRateLimiter rateLimiter = entry.getValue();
                writeLimiterMapByTable.put(tableName, rateLimiter);
                rateLimiter.init(tableCache.getTable(tableName));
                rateLimiter.acquire();
            }
        }

        for (List<T> batch : objectsBatch) {
            logger.debug("Sending batch to save of size: {}", batch.size());
            Map<String, TableWriteItems> tableWriteItems = new HashMap<>();

            for (DynamapRecordBean object : batch) {
                TableDefinition tableDefinition = schemaRegistry.getTableDefinition(object.getClass());
                Item item = new DynamoItemFactory(objectMapper).asDynamoItem(object, tableDefinition);

                String tableName = tableDefinition.getTableName(prefix, batchSaveParams.getSuffix());
                TableWriteItems writeItems = tableWriteItems.getOrDefault(tableName, new TableWriteItems(tableName));
                tableWriteItems.put(tableName, writeItems.addItemToPut(item));
            }

            if (writeLimiterMapByTable != null) {
                for (Map.Entry<String, DynamoRateLimiter> entry : writeLimiterMapByTable.entrySet()) {
                    DynamoRateLimiter rateLimiter = entry.getValue();
                    logger.debug("rateLimiter: about to acquire: {} for table: {}", entry.getValue().getPermitsToConsume(), entry.getKey());
                    rateLimiter.init(tableCache.getTable(entry.getKey()));
                    rateLimiter.acquire();
                }
            }
            doBatchWriteItem(writeLimiterMapByTable, tableWriteItems);
        }
    }

    private void doBatchWriteItem(Map<String, DynamoRateLimiter> writeLimiterMap, Map<String, TableWriteItems> tableWriteItems) {
        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec()
                .withTableWriteItems(tableWriteItems.values().toArray(new TableWriteItems[0]));


        int unprocessedItemsCount;
        do {
            if (writeLimiterMap != null) {
                batchWriteItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            }
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(batchWriteItemSpec);

            if (writeLimiterMap != null) {
                for (ConsumedCapacity consumedCapacity : outcome.getBatchWriteItemResult().getConsumedCapacity()) {
                    DynamoRateLimiter rateLimiter = writeLimiterMap.get(consumedCapacity.getTableName());
                    if (rateLimiter != null) {
                        rateLimiter.setConsumedCapacity(consumedCapacity);
                    }
                }
            }

            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            unprocessedItemsCount = unprocessedItems.size();
            if (unprocessedItemsCount > 0) {
                logger.debug("Retrieving unprocessed items, size: {}", unprocessedItems.size());
                batchWriteItemSpec = new BatchWriteItemSpec().withUnprocessedItems(unprocessedItems);
                if (writeLimiterMap != null) {
                    for (Map.Entry<String, DynamoRateLimiter> entry : writeLimiterMap.entrySet()) {
                        DynamoRateLimiter rateLimiter = entry.getValue();
                        rateLimiter.init(tableCache.getTable(entry.getKey()));
                        rateLimiter.acquire();
                    }
                }
            }
        } while (unprocessedItemsCount > 0);

        logger.debug("doBatchWriteItem done");
    }

    public WriteTx newWriteTx() {
        return new WriteTx(amazonDynamoDB, writeOpFactory, new DynamoItemFactory(objectMapper));
    }

    public ReadTx newReadTx() {
        return new ReadTx(amazonDynamoDB, readOpFactory, new DynamapLoadService(schemaRegistry, dynamapBeanFactory, objectMapper, prefix, tableCache));
    }
}
