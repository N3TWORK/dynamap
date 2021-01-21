package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * All the logic necessary to save Dynamap beans in DynamoDB.
 */
class DynamapSaveService {
    private static final Logger logger = LoggerFactory.getLogger(DynamapSaveService.class);
    private final ObjectMapper objectMapper;
    private final String tableNamePrefix;
    private final TableCache tableCache;

    public DynamapSaveService(ObjectMapper objectMapper, String tableNamePrefix, TableCache tableCache) {
        if (null == objectMapper) {
            throw new IllegalArgumentException();
        }
        this.objectMapper = objectMapper;
        this.tableNamePrefix = tableNamePrefix; // nullable
        if (null == tableCache) {
            throw new IllegalArgumentException();
        }
        this.tableCache = tableCache;
    }

    public <T extends DynamapRecordBean> void saveBean(T bean, TableDefinition tableDefinition, boolean overwrite,
                                                       boolean disableOptimisticLocking, boolean isMigration,
                                                       DynamoRateLimiter writeLimiter, String suffix,
                                                       List<String> paramConditionExpressions, Map<String, String> names, Map<String, Object> values) {
        Item item = new DynamoItemFactory(objectMapper, disableOptimisticLocking).asDynamoItem(bean, tableDefinition);
        PutItemSpec putItemSpec = new PutItemSpec()
                .withItem(item)
                .withReturnValues(ReturnValue.NONE);
        String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
        ValueMap valueMap = new ValueMap();
        NameMap nameMap = new NameMap();
        List<String> conditionalExpressions = new ArrayList<>();
        if (paramConditionExpressions != null) {
            conditionalExpressions.addAll(paramConditionExpressions);
        }
        if (names != null) {
            nameMap.putAll(names);
        }
        if (values != null) {
            valueMap.putAll(values);
        }
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

        if (isMigration) {
            conditionalExpressions.add("#namemigr < :valmigr");
            nameMap.with("#namemigr", tableDefinition.getSchemaVersionField());
            valueMap.withInt(":valmigr", tableDefinition.getVersion());
        }

        if (conditionalExpressions.size() > 0) {
            putItemSpec.withConditionExpression(String.join(" AND ", conditionalExpressions));
            if (valueMap.size() > 0) {
                putItemSpec.withNameMap(nameMap);
                putItemSpec.withValueMap(valueMap);
            }
        }

        Table table = tableCache.getTable(tableDefinition.getTableName(tableNamePrefix, suffix));
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

}
