package com.n3twork.dynamap.tx;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.DeleteRequest;
import com.n3twork.dynamap.*;
import com.n3twork.dynamap.model.TableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The DynamoDB write transactions API works in terms of Put, Update, and Delete instances.
 * This class provides methods to build each of those from our Dynamap types.
 */
public class WriteOpFactory {
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

    public <T extends DynamapRecordBean> Put buildPut(T dynamapRecordBean, DynamoItemFactory dynamoItemFactory) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(dynamapRecordBean.getClass());
        return new Put()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withItem(ItemUtils.toAttributeValues(dynamoItemFactory.asDynamoItem(dynamapRecordBean, tableDefinition)));
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
