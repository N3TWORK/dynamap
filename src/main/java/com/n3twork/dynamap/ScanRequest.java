package com.n3twork.dynamap;

import java.util.Map;

public class ScanRequest<T> {

    private final Class<T> resultClass;
    private DynamapRecordBean.GlobalSecondaryIndexEnum index;
    private String projectionExpression;
    private String filterExpression;
    private Map<String, Object> values;
    private Map<String, String> names;
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

    public ScanRequest<T> withNames(Map<String, String> names) {
        this.names = names;
        return this;
    }

    public ScanRequest<T> withValues(Map<String, Object> values) {
        this.values = values;
        return this;
    }

    public ScanRequest<T> withProjectionExpression(String projectionExpression) {
        this.projectionExpression = projectionExpression;
        return this;
    }

    public ScanRequest<T> withReadRateLimiter(DynamoRateLimiter readRateLimiter) {
        this.readRateLimiter = readRateLimiter;
        return this;
    }

    public ScanRequest<T> withFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
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

    public String getProjectionExpression() {
        return projectionExpression;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Map<String, String> getNames() {
        return names;
    }

    public String getFilterExpression() {
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
