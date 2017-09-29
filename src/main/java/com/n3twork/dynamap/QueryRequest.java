package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;

import java.util.ArrayList;
import java.util.List;

public class QueryRequest<T> {

    private final Class<T> resultClass;
    private DynamapRecordBean.GlobalSecondaryIndexEnum index;
    private String hashKeyValue;
    private RangeKeyCondition rangeKeyCondition;
    private List<QueryFilter> queryFilters = new ArrayList();
    private DynamoRateLimiter readRateLimiter;
    private boolean consistentRead;
    private boolean scanIndexForward = true;
    private Integer limit;
    private Object migrationContext;
    private ProgressCallback progressCallback;
    private boolean writeMigrationChange = false;
    private String suffix;

    public QueryRequest(Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    public QueryRequest<T> withIndex(DynamapRecordBean.GlobalSecondaryIndexEnum index) {
        this.index = index;
        return this;
    }

    public QueryRequest<T> withReadRateLimiter(DynamoRateLimiter readRateLimiter) {
        this.readRateLimiter = readRateLimiter;
        return this;
    }

    public QueryRequest<T> withHashKeyValue(String hashKeyValue) {
        this.hashKeyValue = hashKeyValue;
        return this;
    }

    public QueryRequest<T> withRangeKeyCondition(RangeKeyCondition rangeKeyCondition) {
        this.rangeKeyCondition = rangeKeyCondition;
        return this;
    }

    public QueryRequest<T> withConsistentRead(boolean consistentRead) {
        this.consistentRead = consistentRead;
        return this;
    }

    public QueryRequest<T> addQueryFilter(QueryFilter queryFilter) {
        this.queryFilters.add(queryFilter);
        return this;
    }

    public QueryRequest<T> withScanIndexForward(boolean scanIndexForward) {
        this.scanIndexForward = scanIndexForward;
        return this;
    }

    public QueryRequest<T> withLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public QueryRequest<T> withMigrationContext(Object migrationContext) {
        this.migrationContext = migrationContext;
        return this;
    }

    public QueryRequest<T> withProgressCallback(ProgressCallback progressCallback) {
        this.progressCallback = progressCallback;
        return this;
    }


    public QueryRequest<T> writeMigrationChange(boolean writeMigrationChange) {
        this.writeMigrationChange = writeMigrationChange;
        return this;
    }

    public QueryRequest<T> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
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

    public String getHashKeyValue() {
        return hashKeyValue;
    }

    public RangeKeyCondition getRangeKeyCondition() {
        return rangeKeyCondition;
    }

    public QueryFilter[] getQueryFilters() {
        return queryFilters.toArray(new QueryFilter[queryFilters.size()]);
    }

    public boolean isConsistentRead() {
        return consistentRead;
    }

    public boolean isScanIndexForward() {
        return scanIndexForward;
    }

    public Integer getLimit() {
        return limit;
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

    public String getSuffix() {
        return suffix;
    }
}
