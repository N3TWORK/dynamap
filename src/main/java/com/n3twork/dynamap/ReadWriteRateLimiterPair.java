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
