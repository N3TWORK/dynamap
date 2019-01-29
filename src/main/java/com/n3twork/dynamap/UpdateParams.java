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

public class UpdateParams<T extends DynamapPersisted<? extends RecordUpdates<T>>> {

    private RecordUpdates<T> updates;
    private DynamoRateLimiter writeLimiter;
    private String suffix;
    private DynamapReturnValue dynamapReturnValue = DynamapReturnValue.ALL_NEW;

    private UpdateParams() {
    }

    public UpdateParams(RecordUpdates<T> updates) {
        this.updates = updates;
    }

    public UpdateParams<T> withWriteLimiter(DynamoRateLimiter writeLimiter) {
        this.writeLimiter = writeLimiter;
        return this;
    }

    public UpdateParams<T> withSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }

    public UpdateParams<T> withReturnValue(DynamapReturnValue dynamapReturnValue) {
        this.dynamapReturnValue = dynamapReturnValue;
        return this;
    }

    ////////


    public RecordUpdates<T> getUpdates() {
        return updates;
    }

    public DynamoRateLimiter getWriteLimiter() {
        return writeLimiter;
    }

    public String getSuffix() {
        return suffix;
    }

    public DynamapReturnValue getDynamapReturnValue() {
        return dynamapReturnValue;
    }
}
