package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.n3twork.dynamap.model.TableDefinition;

import java.util.HashMap;
import java.util.Map;

class TxUtil {
    /**
     * @return A Map of key attributes as expected by the low level DynamoDB API.
     */
    static Map<String, AttributeValue> getKey(TableDefinition tableDefinition, String hashKeyValue, Object rangeKeyValue) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(tableDefinition.getHashKey(), new AttributeValue(hashKeyValue));
        if (null != tableDefinition.getRangeKey()) {
            key.put(tableDefinition.getRangeKey(), ItemUtils.toAttributeValue(rangeKeyValue));
        }
        return key;
    }
}
