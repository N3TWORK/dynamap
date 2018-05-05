package com.n3twork.dynamap;

public class UpdateParams<T extends DynamapPersisted> {

    private Updates<T> updates;
    private DynamoRateLimiter writeLimiter;
    private String suffix;
    private DynamapReturnValue dynamapReturnValue = DynamapReturnValue.ALL_NEW;

    private UpdateParams() {
    }

    public UpdateParams(Updates<T> updates) {
        this.updates = updates;
    }

    public UpdateParams<T> withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public UpdateParams<T> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    public UpdateParams<T> withReturnValue(DynamapReturnValue dynamapReturnValue) {
        this.dynamapReturnValue = dynamapReturnValue;
        return this;
    }

    ////////


    public Updates<T> getUpdates() {
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
