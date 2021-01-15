package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.TableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The DynamoDB write transactions API works in terms of Put, Update, and Delete instances.
 * This class provides methods to build each of those from our Dynamap types.
 */
class WriteOpFactory {
    private static final Logger logger = LoggerFactory.getLogger(WriteOpFactory.class);
    private final ObjectMapper objectMapper;
    private final String tableNamePrefix;
    private final SchemaRegistry schemaRegistry;

    public WriteOpFactory(ObjectMapper objectMapper, String tableNamePrefix, SchemaRegistry schemaRegistry) {
        if (null == objectMapper) {
            throw new NullPointerException();
        }
        this.objectMapper = objectMapper;
        this.tableNamePrefix = tableNamePrefix; // nullable
        if (null == schemaRegistry) {
            throw new NullPointerException();
        }
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * This method is deprecated: use buildPut(SaveParams<T>) instead.
     * @Deprecated
     * @param dynamapRecordBean
     * @param dynamoItemFactory
     * @param <T>
     * @return
     */
    public <T extends DynamapRecordBean> Put buildPut(T dynamapRecordBean, DynamoItemFactory dynamoItemFactory) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(dynamapRecordBean.getClass());
        return new Put()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withItem(ItemUtils.toAttributeValues(dynamoItemFactory.asDynamoItem(dynamapRecordBean, tableDefinition)));
    }

    public <T extends DynamapRecordBean> Put buildPut(SaveParams<T> saveParams, DynamoItemFactory dynamoItemFactory) {
        T dynamapRecordBean = saveParams.getDynamapRecordBean();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(dynamapRecordBean.getClass());
        String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
        // Some code duplication between here and DynamapSaveService, TODO clean it up
        List<String> conditionalExpressions = new ArrayList<>();
        boolean overwrite = !saveParams.isDisableOverwrite();
        if (!overwrite) {
            conditionalExpressions.add("attribute_not_exists(" + hashKeyFieldName + ")");
        }
        Put put = new Put()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withItem(ItemUtils.toAttributeValues(dynamoItemFactory.asDynamoItem(dynamapRecordBean, tableDefinition)));
        if (conditionalExpressions.size() > 0) {
            put.withConditionExpression(String.join(" AND ", conditionalExpressions));
        }
        return put;
    }

    public <T extends DynamapPersisted<U>, U extends RecordUpdates<T>> Update buildUpdate(UpdateParams<T> updateParams) {
        RecordUpdates<T> updates = updateParams.getUpdates();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(updates.getTableName());
        Map<String, AttributeValue> key = TxUtil.getKey(tableDefinition, updates.getHashKeyValue(), updates.getRangeKeyValue());

        DynamoExpressionBuilder expressionBuilder = updates.getExpressionBuilder();
        expressionBuilder.setObjectMapper(objectMapper);
        updates.processUpdateExpression();

        return new Update()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withKey(key)
                .withUpdateExpression(expressionBuilder.buildUpdateExpression())
                .withConditionExpression(expressionBuilder.buildConditionalExpression())
                .withExpressionAttributeNames(expressionBuilder.getNameMap())
                .withExpressionAttributeValues(ItemUtils.fromSimpleMap(expressionBuilder.getValueMap()));
    }

    public Delete buildDelete(DeleteRequest deleteRequest) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(deleteRequest.getResultClass());
        Map<String, AttributeValue> key = TxUtil.getKey(tableDefinition, deleteRequest.getHashKeyValue(), deleteRequest.getRangeKeyValue());
        return new Delete()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withKey(key);
    }

    public <T extends DynamapRecordBean> ConditionCheck buildConditionCheck(WriteConditionCheck<T> writeConditionCheck) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(writeConditionCheck.getBeanClass());
        ConditionCheck conditionCheck = new ConditionCheck()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withKey(TxUtil.getKey(tableDefinition, writeConditionCheck.getHashKey(), writeConditionCheck.getRangeKey()))
                .withConditionExpression(writeConditionCheck.getDynamoExpressionBuilder().buildConditionalExpression());
        if (!writeConditionCheck.getDynamoExpressionBuilder().getNameMap().isEmpty()) {
            conditionCheck = conditionCheck.withExpressionAttributeNames(writeConditionCheck.getDynamoExpressionBuilder().getNameMap());
        }
        if (!writeConditionCheck.getDynamoExpressionBuilder().getValueMap().isEmpty()) {
            conditionCheck = conditionCheck.withExpressionAttributeValues(ItemUtils.fromSimpleMap(writeConditionCheck.getDynamoExpressionBuilder().getValueMap()));
        }
        return conditionCheck;
    }
}
