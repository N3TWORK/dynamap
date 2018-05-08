package com.n3twork.dynamap;

import java.util.Map;

public class ScanRequest<T> {

    private final Class<T> resultClass;
    private DynamapRecordBean.SecondaryIndexEnum index;
    private String projectionExpression;
    private String filterExpression;
    private Map<String, Object> values;
    private Map<String, String> names;
    private DynamoRateLimiter readRateLimiter;
    private String startExclusiveHashKeyValue;
    private Object startExclusiveRangeKeyValue;
    private Object migrationContext;
    private Integer maxResultSize;
    private ProgressCallback progressCallback;
    private boolean writeMigrationChange;
    private String suffix;

    public ScanRequest(Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    public ScanRequest<T> withIndex(DynamapRecordBean.SecondaryIndexEnum index) {
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
        this.startExclusiveRangeKeyValue = startExclusiveHashKeyValue;
        return this;
    }

    public ScanRequest<T> withStartExclusiveRangeKeyValue(Object startExclusiveRangeKeyValue) {
        this.startExclusiveRangeKeyValue = startExclusiveRangeKeyValue;
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

    public ScanRequest<T> writeMigrationChange(boolean writeMigrationChange) {
        this.writeMigrationChange = writeMigrationChange;
        return this;
    }

    public ScanRequest<T> withSuffix(String suffix) {
        this.suffix = suffix;
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

    public String getStartExclusiveHashKeyValue() {
        return startExclusiveHashKeyValue;
    }

    public Object getStartExclusiveRangeKeyValue() {
        return startExclusiveRangeKeyValue;
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

    public DynamapRecordBean.SecondaryIndexEnum getIndex() {
        return index;
    }

    public Integer getMaxResultSize() {
        return maxResultSize;
    }

    public boolean isWriteMigrationChange() {
        return writeMigrationChange;
    }

    public String getSuffix() {
        return suffix;
    }
}
