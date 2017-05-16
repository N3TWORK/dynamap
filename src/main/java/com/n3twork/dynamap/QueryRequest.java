package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;

import java.util.ArrayList;
import java.util.List;

public class QueryRequest<T extends DynamapPersisted> {

    private final Class<T> resultClass;
    private String index;
    private String hashKeyValue;
    private RangeKeyCondition rangeKeyCondition;
    private List<QueryFilter> queryFilters = new ArrayList();
    private DynamoRateLimiter readRateLimiter;
    private boolean consistentRead;

    public QueryRequest(Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    public QueryRequest<T> withIndex(String index) {
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


    public DynamoRateLimiter getReadRateLimiter() {
        return readRateLimiter;
    }

    public Class<T> getResultClass() {
        return resultClass;
    }

    public String getIndex() {
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
}
