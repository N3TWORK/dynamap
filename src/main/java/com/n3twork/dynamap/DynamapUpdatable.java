package com.n3twork.dynamap;

public interface DynamapUpdatable<T extends Updates<?>> extends DynamapPersisted {

    default T asUpdates(String hashKey, Object rangeKey) {
        return asUpdates(hashKey, rangeKey, false);
    }

    T asUpdates(String hashKey, Object rangeKey, boolean disableOptimisticLocking);
}
