/*
    Copyright 2018 N3TWORK INC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.document.KeyAttribute;

import java.util.Map;

public class ScanRequest<T> {

    private final Class<T> resultClass;
    private DynamapRecordBean.SecondaryIndexEnum index;
    private String projectionExpression;
    private String filterExpression;
    private Map<String, Object> values;
    private Map<String, String> names;
    private DynamoRateLimiter readRateLimiter;
    private KeyAttribute[] exclusiveStartKeys;
    private Integer segment;
    private Integer totalSegments;
    private Object migrationContext;
    private Integer maxResultSize;
    private Integer maxPageSize;
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

    public ScanRequest<T> withExclusiveStartKeys(KeyAttribute... exclusiveStartKeys) {
        this.exclusiveStartKeys = exclusiveStartKeys;
        return this;
    }

    public ScanRequest<T> withSegment(Integer segment) {
        this.segment = segment;
        return this;
    }

    public ScanRequest<T> withTotalSegments(Integer totalSegments) {
        this.totalSegments = totalSegments;
        return this;
    }

    public ScanRequest<T> withMaxResultSize(Integer maxResultSize) {
        this.maxResultSize = maxResultSize;
        return this;
    }

    public ScanRequest<T> withMaxPageSize(Integer maxPageSize) {
        this.maxPageSize = maxPageSize;
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

    public KeyAttribute[] getExclusiveStartKeys() {
        return exclusiveStartKeys;
    }

    public Integer getSegment() {
        return segment;
    }

    public Integer getTotalSegments() {
        return totalSegments;
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

    public Integer getMaxPageSize() {
        return maxPageSize;
    }

    public boolean isWriteMigrationChange() {
        return writeMigrationChange;
    }

    public String getSuffix() {
        return suffix;
    }
}
