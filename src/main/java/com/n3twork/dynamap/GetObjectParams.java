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

public class GetObjectParams<T extends DynamapRecordBean> {

    private GetObjectRequest<T> getObjectRequest;

    private ReadWriteRateLimiterPair rateLimiters;
    private Object migrationContext;
    private boolean writeMigrationChange = true;

    public GetObjectParams(GetObjectRequest<T> getObjectRequest) {
        this.getObjectRequest = getObjectRequest;
    }

    public GetObjectParams<T> withRateLimiters(ReadWriteRateLimiterPair readWriteRateLimiterPair) {
        this.rateLimiters = readWriteRateLimiterPair;
        return this;
    }

    public GetObjectParams<T> withMigrationContext(Object migrationContext) {
        this.migrationContext = migrationContext;
        return this;
    }

    public GetObjectParams<T> withWriteMigrationChange(boolean writeMigrationChange) {
        this.writeMigrationChange = writeMigrationChange;
        return this;
    }

    public GetObjectRequest<T> getGetObjectRequest() {
        return getObjectRequest;
    }

    public ReadWriteRateLimiterPair getRateLimiters() {
        return rateLimiters;
    }

    public Object getMigrationContext() {
        return migrationContext;
    }

    public boolean isWriteMigrationChange() {
        return writeMigrationChange;
    }
}
