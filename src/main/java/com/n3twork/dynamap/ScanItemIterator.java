package com.n3twork.dynamap;

import java.util.Iterator;

public interface ScanItemIterator<T> extends Iterator<T> {

    String getLastHashKey();

    Object getLastRangeKey();

    int getCount();

    int getScannedCount();

}
