package com.n3twork.dynamap;

public interface DynamapRecordBean<T extends DynamapPersisted> {

    String getHashKeyValue();

    Object getRangeKeyValue();

    interface SecondaryIndexEnum {
        String getName();
    }
}
