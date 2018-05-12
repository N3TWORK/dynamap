package com.n3twork.dynamap;

public interface DynamapRecordBean<T extends Updates<?>> extends DynamapUpdatable<T> {

    String getHashKeyValue();

    Object getRangeKeyValue();

    default T asUpdates() {
        return asUpdates(getHashKeyValue(), getRangeKeyValue());
    }

    default T asUpdates(boolean disableOptimisticLocking) {
        return asUpdates(getHashKeyValue(), getRangeKeyValue(), disableOptimisticLocking);
    }

    interface SecondaryIndexEnum {
        String getName();
    }
}