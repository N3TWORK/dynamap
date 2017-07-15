package com.n3twork.dynamap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BatchGetObjectRequest<T extends DynamapRecordBean> {

    private Collection<GetObjectRequest<T>> getObjectRequests;
    private Map<Class, ReadWriteRateLimiterPair> rateLimiters = new HashMap<>();
    private ReadWriteRateLimiterPair readWriteRateLimiterPair;
    private Object migrationContext;
    private ProgressCallback progressCallback;
    private boolean writeMigrationChange = true;

    public BatchGetObjectRequest<T> withGetObjectRequests(Collection getObjectRequests) {
        this.getObjectRequests = getObjectRequests;
        return this;
    }

    public BatchGetObjectRequest<T> withRateLimiters(Map<Class, ReadWriteRateLimiterPair> rateLimiters) {
        this.rateLimiters = rateLimiters;
        return this;
    }

    public BatchGetObjectRequest<T> withRateLimiters(ReadWriteRateLimiterPair rateLimiters) {
        this.readWriteRateLimiterPair = rateLimiters;
        return this;
    }

    public BatchGetObjectRequest<T> withMigrationContext(Object migrationContext) {
        this.migrationContext = migrationContext;
        return this;
    }

    public BatchGetObjectRequest<T> withProgressCallback(ProgressCallback progressCallback) {
        this.progressCallback = progressCallback;
        return this;
    }

    public BatchGetObjectRequest<T> writeMigrationChange(boolean writeMigrationChange) {
        this.writeMigrationChange = writeMigrationChange;
        return this;
    }

    public Collection<GetObjectRequest<T>> getGetObjectRequests() {
        return getObjectRequests;
    }

    public Map<Class, ReadWriteRateLimiterPair> getRateLimiters() {
        return rateLimiters;
    }

    public ReadWriteRateLimiterPair getReadWriteRateLimiterPair() {
        return readWriteRateLimiterPair;
    }

    public Object getMigrationContext() {
        return migrationContext;
    }

    public ProgressCallback getProgressCallback() {
        return progressCallback;
    }

    public boolean isWriteMigrationChange() {
        return writeMigrationChange;
    }
}
