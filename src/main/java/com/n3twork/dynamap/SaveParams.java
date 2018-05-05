package com.n3twork.dynamap;

public class SaveParams<T extends DynamapRecordBean> {

    private T dynamapRecordBean;

    private boolean disableOverwrite;
    private boolean disableOptimisticLocking;
    private DynamoRateLimiter writeLimiter;
    private String suffix;

    private SaveParams() {
    }

    public SaveParams(T dynamapRecordBean) {
        this.dynamapRecordBean = dynamapRecordBean;
    }

    public SaveParams withDisableOverwrite(boolean disableOverwrite) {
        this.disableOverwrite = disableOverwrite;
        return this;
    }

    public SaveParams withDisableOptimisticLocking(boolean disableOptimisticLocking) {
        this.disableOptimisticLocking = disableOptimisticLocking;
        return this;
    }

    public SaveParams withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public SaveParams withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    ////////


    public T getDynamapRecordBean() {
        return dynamapRecordBean;
    }

    public boolean isDisableOverwrite() {
        return disableOverwrite;
    }

    public boolean isDisableOptimisticLocking() {
        return disableOptimisticLocking;
    }

    public DynamoRateLimiter getWriteLimiter() {
        return writeLimiter;
    }

    public String getSuffix() {
        return suffix;
    }
}
