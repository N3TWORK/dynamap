package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.TableDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds DynamapRecordBean instances from DynamoDB Items.
 */
class DynamapBeanFactory {
    private final SchemaRegistry schemaRegistry;
    private final ObjectMapper objectMapper;

    public DynamapBeanFactory(SchemaRegistry schemaRegistry, ObjectMapper objectMapper) {
        if (null == schemaRegistry) {
            throw new IllegalArgumentException();
        }
        this.schemaRegistry = schemaRegistry;
        if (null == objectMapper) {
            throw new IllegalArgumentException();
        }
        this.objectMapper = objectMapper;
    }

    public <T extends DynamapRecordBean> T asDynamapBean(Item item, Class<T> resultClass) {
        TableDefinition tableDefinition = schemaRegistry.getTableDefinition(resultClass);
        Map<String, Object> itemMap = item.asMap();
        processDeserializationConversions(tableDefinition, itemMap);
        return objectMapper.convertValue(itemMap, resultClass);
    }

    private void processDeserializationConversions(TableDefinition tableDefinition, Map<String, Object> map) {
        // decompress gzip byte arrays
        for (TableDefinition.CompressCollectionItem compressCollectionItem : tableDefinition.getCompressCollectionItems()) {
            byte[] bytes = null;
            if (compressCollectionItem.parentKey != null) {
                Map<String, Object> parent = (Map<String, Object>) map.get(compressCollectionItem.parentKey);
                if (parent != null) {
                    bytes = (byte[]) parent.get(compressCollectionItem.itemKey);
                }
            } else {
                bytes = (byte[]) map.get(compressCollectionItem.itemKey);
            }
            if (bytes != null) {
                Object object = GZipUtil.deSerialize(bytes, objectMapper, Object.class);
                if (compressCollectionItem.parentKey != null) {
                    ((Map) map.get(compressCollectionItem.parentKey)).put(compressCollectionItem.itemKey, object);
                } else {
                    map.put(compressCollectionItem.itemKey, object);
                }
            }
        }

        // convert lists to map for persistAsList fields
        for (TableDefinition.PersistAsFieldItem persistAsFieldItem : tableDefinition.getPersistAsFieldItems()) {
            List<Map<String, Object>> list = null;
            if (persistAsFieldItem.parentKey != null) {
                Map<String, Object> parent = (Map<String, Object>) map.get(persistAsFieldItem.parentKey);
                if (parent != null) {
                    list = (List<Map<String, Object>>) parent.get(persistAsFieldItem.itemKey);
                }
            } else {
                list = (List<Map<String, Object>>) map.get(persistAsFieldItem.itemKey);
            }
            if (list != null) {
                Map<String, Object> converted = new HashMap<>();
                for (Map<String, Object> item : list) {
                    converted.put((String) item.get(persistAsFieldItem.idKey), item);
                }
                if (persistAsFieldItem.parentKey != null) {
                    ((Map) map.get(persistAsFieldItem.parentKey)).put(persistAsFieldItem.itemKey, converted);
                } else {
                    map.put(persistAsFieldItem.itemKey, converted);
                }
            }
        }
    }
}
