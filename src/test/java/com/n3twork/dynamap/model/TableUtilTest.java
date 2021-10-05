package com.n3twork.dynamap.model;

import org.testng.annotations.Test;

import static com.n3twork.dynamap.model.TableUtil.getTableName;
import static org.testng.Assert.assertEquals;

public class TableUtilTest {

    @Test
    public void noPrefixNoSuffix() {
        assertEquals(getTableName("tableName", null, null), "tableName");
    }

    @Test
    public void withPrefix() {
        assertEquals(getTableName("tableName", "prefix.", null), "prefix.tableName");
    }

    @Test
    public void withSuffix() {
        assertEquals(getTableName("tableName", null, ".suffix"), "tableName.suffix");
    }

    @Test
    public void withPrefixAndSuffix() {
        assertEquals(getTableName("tableName", "prefix.", ".suffix"), "prefix.tableName.suffix");
    }
}