package com.n3twork.dynamap;

import java.util.List;

public class ScanResult<T> {

    private final String lastEvaluatedHashKey;
    private final Object lastEvaluatedRangeKey;
    private final List<T> results;
    private final int count;
    private final int scannedCount;

    public ScanResult(String lastEvaluatedHashKey, Object lastEvaluatedRangeKey, List<T> results, int count, int scannedCount) {
        this.lastEvaluatedHashKey = lastEvaluatedHashKey;
        this.lastEvaluatedRangeKey = lastEvaluatedRangeKey;
        this.results = results;
        this.count = count;
        this.scannedCount = scannedCount;
    }

    public String getLastEvaluatedHashKey() {
        return lastEvaluatedHashKey;
    }

    public Object getLastEvaluatedRangeKey() {
        return lastEvaluatedRangeKey;
    }

    public List<T> getResults() {
        return results;
    }

    public int getCount() {
        return count;
    }

    public int getScannedCount() {
        return scannedCount;
    }
}
