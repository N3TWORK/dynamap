package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.Field;
import com.n3twork.dynamap.model.Schema;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.model.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DynamoItemFactory {
    private final ObjectMapper objectMapper;
    private final boolean disableOptimisticLocking;

    public DynamoItemFactory(ObjectMapper objectMapper) {
        if (null == objectMapper) {
            throw new IllegalArgumentException();
        }
        this.objectMapper = objectMapper;
        this.disableOptimisticLocking = false;
    }

    public DynamoItemFactory(ObjectMapper objectMapper, boolean disableOptimisticLocking) {
        this.objectMapper = objectMapper;
        this.disableOptimisticLocking = disableOptimisticLocking;
    }

    public <T extends DynamapRecordBean> Item asDynamoItem(T object, TableDefinition tableDefinition) {
        Map<String, Object> map = objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {
        });
        Item item = new Item();
        if (tableDefinition.isEnableMigrations()) {
            item.withInt(tableDefinition.getSchemaVersionField(), tableDefinition.getVersion());
        }

        if (!disableOptimisticLocking && tableDefinition.isOptimisticLocking()) {
            int revision = (int) map.getOrDefault(Schema.REVISION_FIELD, 0);
            item.withInt(Schema.REVISION_FIELD, revision + 1);
        }

        Type type = tableDefinition.getTypes().stream().filter(t -> t.getName().equals(tableDefinition.getType())).findFirst().get();
        for (Field field : type.getPersistedFields()) {
            if (!map.containsKey(field.getDynamoName()) || map.get(field.getDynamoName()) == null) {
                continue;
            }
            // if field is a nested dynamap object then remove any non persisted fields
            if (field.isGeneratedType()) {
                Map<String, Object> objectToPersist = (Map<String, Object>) map.get(field.getDynamoName());
                Type fieldType = tableDefinition.getFieldType(field.getElementType());
                for (Field fieldToCheck : fieldType.getFields()) {
                    if (objectToPersist.containsKey(fieldToCheck.getName()) && !fieldToCheck.isPersist()) {
                        objectToPersist.remove(fieldToCheck.getName());
                    }
                }
            }
            // Jackson converts all collections to array lists, which in turn are treated as lists
            // Need to handle Sets specifically. Sets can also not be empty
            if (field.getType().equals("Set")) {
                List list = (List) map.get(field.getDynamoName());
                if (list.size() > 0) {
                    item.with(field.getDynamoName(), new HashSet<>((List) map.get(field.getDynamoName())));
                }
            } else {
                item.with(field.getDynamoName(), map.get(field.getDynamoName()));
            }
        }

        processSerializationConversions(tableDefinition, item);

        String hashKeyFieldName = tableDefinition.getField(tableDefinition.getHashKey()).getDynamoName();
        if (object.getRangeKeyValue() != null) {
            String rangeKeyFieldName = tableDefinition.getField(tableDefinition.getRangeKey()).getDynamoName();
            item.withPrimaryKey(hashKeyFieldName, object.getHashKeyValue(), rangeKeyFieldName, object.getRangeKeyValue());
        } else {
            item.withPrimaryKey(hashKeyFieldName, object.getHashKeyValue());
        }

        return item;
    }

    private void processSerializationConversions(TableDefinition tableDefinition, Item item) {
        // convert maps to list for persistAsList fields
        for (TableDefinition.PersistAsFieldItem persistAsFieldItem : tableDefinition.getPersistAsFieldItems()) {
            Map<String, Object> map = null;
            if (persistAsFieldItem.parentKey != null) {
                Map<String, Object> parent = (Map<String, Object>) item.get(persistAsFieldItem.parentKey);
                if (parent != null) {
                    map = (Map<String, Object>) parent.get(persistAsFieldItem.itemKey);
                }
            } else {
                map = (Map<String, Object>) item.get(persistAsFieldItem.itemKey);
            }
            if (map != null) {
                List converted = new ArrayList(map.values());
                if (persistAsFieldItem.parentKey != null) {
                    ((Map) item.get(persistAsFieldItem.parentKey)).put(persistAsFieldItem.itemKey, converted);
                } else {
                    item.withList(persistAsFieldItem.itemKey, converted);
                }
            }
        }

        // compress json to byte array
        for (TableDefinition.CompressCollectionItem compressCollectionItem : tableDefinition.getCompressCollectionItems()) {
            Object object = null;
            if (compressCollectionItem.parentKey != null) {
                Map<String, Object> parent = (Map<String, Object>) item.get(compressCollectionItem.parentKey);
                if (parent != null) {
                    object = parent.get(compressCollectionItem.itemKey);
                }
            } else {
                object = item.get(compressCollectionItem.itemKey);
            }
            if (object != null) {
                byte[] bytes = GZipUtil.serialize(object, objectMapper);
                if (compressCollectionItem.parentKey != null) {
                    ((Map) item.get(compressCollectionItem.parentKey)).put(compressCollectionItem.itemKey, bytes);
                } else {
                    item.withBinary(compressCollectionItem.itemKey, bytes);
                }
            }
        }
    }
}
