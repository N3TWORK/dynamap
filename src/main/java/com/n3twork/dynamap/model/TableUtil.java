package com.n3twork.dynamap.model;

public class TableUtil {

    public static String getTableName(String tableName, String prefix, String suffix) {
        String result = tableName;
        if (prefix != null) {
            result = prefix + result;
        }
        if (suffix != null) {
            result += suffix;
        }
        return result;
    }
}
