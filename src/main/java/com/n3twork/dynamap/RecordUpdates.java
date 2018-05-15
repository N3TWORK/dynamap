package com.n3twork.dynamap;

public interface RecordUpdates<T extends DynamapPersisted> extends Updates,DynamapRecordBean {

    String getTableName();

    String getHashKeyValue();

    Object getRangeKeyValue();

}
