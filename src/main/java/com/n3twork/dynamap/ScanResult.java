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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ScanResult<T> {

    private final ItemIterator<T> itemIterator;

    public ScanResult(ItemIterator<T> itemIterator) {
        this.itemIterator = itemIterator;
    }

    public List<T> getResults() {
        List<T> results = new ArrayList<>();
        while (itemIterator.hasNext()) {
            results.add(itemIterator.next());
        }
        return results;
    }

    public Iterator<T> getResultIterator() {
        return itemIterator;
    }

    public int getCount() {
        return itemIterator.getCount();
    }

    public int getScannedCount() {
        return itemIterator.getScannedCount();
    }

    public KeyAttribute[] getLastEvaluatedKeys() {
        return itemIterator.getLastEvaluatedKeys();
    }
}
