package com.n3twork.dynamap;

import com.n3twork.dynamap.model.FilterExpression;

import java.util.Collection;

public class ScanRequest<T> {

    private final Class<T> resultClass;
    private DynamapRecordBean.GlobalSecondaryIndexEnum index;
    private Collection<String> fieldsToGet;
    private FilterExpression filterExpression;
    private DynamoRateLimiter readRateLimiter;
    private String startExclusiveHashKey;
    private Object startExclusiveRangeKey;
    private Object migrationContext;
    private Integer maxResultSize;
    private ProgressCallback progressCallback;

    public ScanRequest(Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    public ScanRequest<T> withIndex(DynamapRecordBean.GlobalSecondaryIndexEnum index) {
        this.index = index;
        return this;
    }

    public ScanRequest<T> withReadRateLimiter(DynamoRateLimiter readRateLimiter) {
        this.readRateLimiter = readRateLimiter;
        return this;
    }

    public ScanRequest<T> withFilterExpression(FilterExpression filterExpression) {
        this.filterExpression = filterExpression;
        return this;
    }

    public ScanRequest<T> withFieldsToGet(Collection<String> fieldsToGet) {
        this.fieldsToGet = fieldsToGet;
        return this;
    }

    public ScanRequest<T> withMigrationContext(Object migrationContext) {
        this.migrationContext = migrationContext;
        return this;
    }

    public ScanRequest<T> withStartExclusiveHashKeyValue(String startExclusiveHashKeyValue) {
        this.startExclusiveHashKey = startExclusiveHashKeyValue;
        return this;
    }

    public ScanRequest<T> withStartExclusiveRangeKeyValue(Object startExclusiveRangeKeyValue) {
        this.startExclusiveRangeKey = startExclusiveRangeKeyValue;
        return this;
    }

    public ScanRequest<T> withMaxResultSize(Integer maxResultSize) {
        this.maxResultSize = maxResultSize;
        return this;
    }

    public ScanRequest<T> withProgressCallback(ProgressCallback progressCallback) {
        this.progressCallback = progressCallback;
        return this;
    }

    public Collection<String> getFieldsToGet() {
        return fieldsToGet;
    }

    public FilterExpression getFilterExpression() {
        return filterExpression;
    }

    public String getStartExclusiveHashKey() {
        return startExclusiveHashKey;
    }

    public Object getStartExclusiveRangeKey() {
        return startExclusiveRangeKey;
    }

    public Object getMigrationContext() {
        return migrationContext;
    }

    public ProgressCallback getProgressCallback() {
        return progressCallback;
    }

    public DynamoRateLimiter getReadRateLimiter() {
        return readRateLimiter;
    }

    public Class<T> getResultClass() {
        return resultClass;
    }

    public DynamapRecordBean.GlobalSecondaryIndexEnum getIndex() {
        return index;
    }

    public Integer getMaxResultSize() {
        return maxResultSize;
    }
}
