package com.n3twork.dynamap;

public class GetObjectParams<T extends DynamapRecordBean> {

    private GetObjectRequest<T> getObjectRequest;

    private ReadWriteRateLimiterPair rateLimiters;
    private Object migrationContext;
    private boolean writeMigrationChange = true;

    public GetObjectParams(GetObjectRequest<T> getObjectRequest) {
        this.getObjectRequest = getObjectRequest;
    }

    public GetObjectParams<T> withRateLimiters(ReadWriteRateLimiterPair readWriteRateLimiterPair) {
        this.rateLimiters = readWriteRateLimiterPair;
        return this;
    }

    public GetObjectParams<T> withWriteMigrationChange(boolean writeMigrationChange) {
        this.writeMigrationChange = writeMigrationChange;
        return this;
    }

    public GetObjectRequest<T> getGetObjectRequest() {
        return getObjectRequest;
    }

    public ReadWriteRateLimiterPair getRateLimiters() {
        return rateLimiters;
    }

    public Object getMigrationContext() {
        return migrationContext;
    }

    public boolean isWriteMigrationChange() {
        return writeMigrationChange;
    }
}
