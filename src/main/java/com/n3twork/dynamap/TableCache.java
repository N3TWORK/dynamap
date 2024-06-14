package com.n3twork.dynamap;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.HashMap;
import java.util.Map;

public class TableCache {
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbClient dynamoDbClient;
    private final Map<String, TableDescription> tableCacheMap = new HashMap<>();

    public TableCache(DynamoDbClient dynamoDbClient) {
        if (null == dynamoDbClient) {
            throw new IllegalArgumentException();
        }
        this.enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
        this.dynamoDbClient = dynamoDbClient;
    }

    public TableDescription getTable(String tableName) {
        return tableCacheMap.computeIfAbsent(tableName, k -> dynamoDbClient.describeTable(r -> r.tableName(tableName)).table());
    }
}
