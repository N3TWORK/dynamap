package com.n3twork.dynamap;

public class ReadWriteRateLimiterPair {

    private final DynamoRateLimiter readLimiter;
    private final DynamoRateLimiter writeLimiter;


    public static ReadWriteRateLimiterPair of(DynamoRateLimiter readLimiter, DynamoRateLimiter writeLimiter) {
        return new ReadWriteRateLimiterPair(readLimiter, writeLimiter);
    }

    private ReadWriteRateLimiterPair(DynamoRateLimiter readLimiter, DynamoRateLimiter writeLimiter) {
        this.readLimiter = readLimiter;
        this.writeLimiter = writeLimiter;
    }

    public DynamoRateLimiter getReadLimiter() {
        return readLimiter;
    }

    public DynamoRateLimiter getWriteLimiter() {
        return writeLimiter;
    }
}
