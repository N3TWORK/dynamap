package com.n3twork.dynamap.tx;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.n3twork.dynamap.DynamapRecordBean;
import com.n3twork.dynamap.DynamoRateLimiter;

public interface ItemToDynamapConverter {
    /**
     * Build a DynamapRecordBean from a Dynamap Item.
     * The Dynamap class supports this behavior internally and this interface allows us
     * to use that behavior to build Dynamap Record Beans from Items we read in a ReadTx.
     */
    <T extends DynamapRecordBean> T toDynamapBean(Item item, Class<T> resultClass, DynamoRateLimiter writeRateLimiter, Object migrationContext, boolean writeBack, boolean skipMigration, String suffix);
}
