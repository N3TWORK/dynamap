package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Iterator;
import java.util.Map;

public abstract class ItemIterator<T> implements Iterator<T> {
    private ItemCollection<?> itemCollection;
    protected IteratorSupport<Item, ?> iterator;

    ItemIterator(ItemCollection<?> itemCollection) {
        this.itemCollection = itemCollection;
        iterator = itemCollection.iterator();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public int getCount() {
        return itemCollection.getAccumulatedItemCount();
    }

    public int getScannedCount() {
        return itemCollection.getAccumulatedScannedCount();
    }

    public abstract T next();

    public KeyAttribute[] getLastEvaluatedKeys() {
        Map<String, AttributeValue> lastEvaluatedKeyMap = getLowLevelLastEvaluatedKey();
        if (lastEvaluatedKeyMap == null) {
            return null;
        }

        KeyAttribute[] lastEvaluatedKeyArray = new KeyAttribute[lastEvaluatedKeyMap.size()];
        int i = 0;
        for (Map.Entry<String, AttributeValue> entry : lastEvaluatedKeyMap.entrySet()) {
            lastEvaluatedKeyArray[i++] = new KeyAttribute(entry.getKey(), ItemUtils.toSimpleValue(entry.getValue()));
        }

        return lastEvaluatedKeyArray;
    }

    protected abstract Map<String, AttributeValue> getLowLevelLastEvaluatedKey();

}
