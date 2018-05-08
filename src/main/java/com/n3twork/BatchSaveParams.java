package com.n3twork;

import com.n3twork.dynamap.DynamapRecordBean;
import com.n3twork.dynamap.DynamoRateLimiter;

import java.util.List;
import java.util.Map;

public class BatchSaveParams<T extends DynamapRecordBean> {

    private List<T> dynamapRecordBeans;

    private boolean disableOverwrite;
    private boolean disableOptimisticLocking;
    private Map<Class, DynamoRateLimiter> writeLimiters;
    private String suffix;

    private BatchSaveParams() {
    }

    public BatchSaveParams(List<T> dynamapRecordBeans) {
        this.dynamapRecordBeans = dynamapRecordBeans;
    }

    public BatchSaveParams<T> withDisableOverwrite(boolean disableOverwrite) {
        this.disableOverwrite = disableOverwrite;
        return this;
    }

    public BatchSaveParams<T> withDisableOptimisticLocking(boolean disableOptimisticLocking) {
        this.disableOptimisticLocking = disableOptimisticLocking;
        return this;
    }

    public BatchSaveParams<T> withWriteLimiters(Map<Class, DynamoRateLimiter> writeLimiters) {
        this.writeLimiters = writeLimiters;
        return this;
    }

    public BatchSaveParams<T> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    ////////


    public List<T> getDynamapRecordBeans() {
        return dynamapRecordBeans;
    }

    public boolean isDisableOverwrite() {
        return disableOverwrite;
    }

    public boolean isDisableOptimisticLocking() {
        return disableOptimisticLocking;
    }

    public Map<Class, DynamoRateLimiter> getWriteLimiters() {
        return writeLimiters;
    }

    public String getSuffix() {
        return suffix;
    }
}
