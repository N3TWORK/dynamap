package com.n3twork.dynamap;

import java.util.Iterator;

public interface ItemIterator<T> extends Iterator<T> {

    int getCount();

    int getScannedCount();

}
