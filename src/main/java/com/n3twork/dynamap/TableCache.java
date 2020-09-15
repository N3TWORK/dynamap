package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.HashMap;
import java.util.Map;

public class TableCache {
    private final DynamoDB dynamoDB;
    private final Map<String, Table> tableCache = new HashMap<>();

    public TableCache(DynamoDB dynamoDB) {
        if (null == dynamoDB) {
            throw new IllegalArgumentException();
        }
        this.dynamoDB = dynamoDB;
    }

    public Table getTable(String tableName) {
        Table table = tableCache.get(tableName);
        if (table == null) {
            table = dynamoDB.getTable(tableName);
            tableCache.put(tableName, table);
        }
        return table;
    }
}
