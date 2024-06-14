package com.n3twork.dynamap.utils;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class TableUtils {

    private TableUtils() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static boolean deleteTableIfExists(DynamoDbClient dynamoDbClient, String tableName) {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            // If the table is found, delete it
            dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
            return true;
        } catch (ResourceNotFoundException e) {
            // If the table was not found, it has been deleted already or it never existed, so we can safely ignore this exception
        }
        return false;
    }

    public static boolean createTableIfNotExists(DynamoDbClient dynamoDbClient, CreateTableRequest createTableRequest) {
        try {
            dynamoDbClient.describeTable(DescribeTableRequest.builder().tableName(createTableRequest.tableName()).build());
            return false;
        } catch (ResourceNotFoundException e) {
            // If the table was not found, create it
            dynamoDbClient.createTable(createTableRequest);
        }
        return true;
    }
}
