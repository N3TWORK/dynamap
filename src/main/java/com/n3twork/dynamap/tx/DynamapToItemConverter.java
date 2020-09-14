package com.n3twork.dynamap.tx;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.n3twork.dynamap.DynamapRecordBean;
import com.n3twork.dynamap.model.TableDefinition;

public interface DynamapToItemConverter {
    /**
     * Build a DynamoDB Item from a DynamapRecordBean. The Dynamap class supports this behavior
     * internally and this interface allows us to use that behavior to convert DynamapRecordBeans
     * to Item when we do Puts in a WriteTx.
     */
    <T extends DynamapRecordBean> Item toItem(T dynamapRecordBean, TableDefinition tableDefinition);
}
