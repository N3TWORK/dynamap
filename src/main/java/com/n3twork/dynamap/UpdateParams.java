package com.n3twork.dynamap;

public class UpdateParams<T extends DynamapRecordBean<?>, U extends Updates<T>> {

    private final U updates;

    private DynamoRateLimiter writeLimiter;
    private String suffix;
    private DynamapReturnValue dynamapReturnValue = DynamapReturnValue.ALL_NEW;

    public UpdateParams(U updates) {
        this.updates = updates;
    }

    public UpdateParams<T, U> withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public UpdateParams<T, U> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    public UpdateParams<T, U> withReturnValue(DynamapReturnValue dynamapReturnValue) {
        this.dynamapReturnValue = dynamapReturnValue;
        return this;
    }

    ////////


    public U getUpdates() {
        return updates;
    }

    public DynamoRateLimiter getWriteLimiter() {
        return writeLimiter;
    }

    public String getSuffix() {
        return suffix;
    }

    public DynamapReturnValue getDynamapReturnValue() {
        return dynamapReturnValue;
    }
}
