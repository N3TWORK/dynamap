package com.n3twork.dynamap;

public class UpdateParams<T extends DynamapPersisted> {

    private Updates<T> updates;
    private DynamoRateLimiter writeLimiter;
    private String suffix;

    private UpdateParams() {
    }

    public UpdateParams(Updates<T> updates) {
        this.updates = updates;
    }

    public UpdateParams withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public UpdateParams withSuffix(String suffix) {
        this.suffix = suffix;
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
}
