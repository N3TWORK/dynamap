package com.n3twork.dynamap;

import com.n3twork.dynamap.model.TableDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Get;

import java.util.Map;

/**
 * The DynamoDB read transactions API works in terms of Get instances.
 * This class provides methods to build Get objects from Dynamap GetObjectParams instances.
 */
class ReadOpFactory {
    private final SchemaRegistry schemaRegistry;
    private final String tableNamePrefix;

    public ReadOpFactory(SchemaRegistry schemaRegistry, String tableNamePrefix) {
        if (null == schemaRegistry) {
            throw new NullPointerException();
        }
        this.schemaRegistry = schemaRegistry;
        this.tableNamePrefix = tableNamePrefix; // nullable
    }

    public <T extends DynamapRecordBean> Get buildGet(GetObjectParams<T> getObjectParams) {
        GetObjectRequest<T> getObjectRequest = getObjectParams.getGetObjectRequest();
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(getObjectRequest.getResultClass());
        Map<String, AttributeValue> key = TxUtil.getKey(tableDefinition, getObjectRequest.getHashKeyValue(), getObjectRequest.getRangeKeyValue());
        return Get.builder()
                .tableName(tableDefinition.getTableName(tableNamePrefix))
                .key(key)
                .build();
    }
}
