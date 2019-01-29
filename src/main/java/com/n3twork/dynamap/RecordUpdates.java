package com.n3twork.dynamap;

public interface RecordUpdates<T extends DynamapPersisted<? extends RecordUpdates<T>>> extends Updates<T>, DynamapRecordBean {

    String getTableName();

    String getHashKeyValue();

    Object getRangeKeyValue();

}
