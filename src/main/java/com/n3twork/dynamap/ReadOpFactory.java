package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Get;
import com.n3twork.dynamap.model.TableDefinition;

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
        return new Get()
                .withTableName(tableDefinition.getTableName(tableNamePrefix))
                .withKey(key);
    }
}
