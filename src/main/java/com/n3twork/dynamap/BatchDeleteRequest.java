/*
    Copyright 2017 N3TWORK INC

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

import java.util.List;
import java.util.Map;

/**
 * Contains the parameters required for making batch deletes
 * @param <T> The record bean type that is being deleted
 */
public class BatchDeleteRequest<T extends DynamapRecordBean> {

    private List<DeleteRequest> deleteRequests;
    private Map<Class, DynamoRateLimiter> rateLimiters;

    /**
     * Sets the list of invidual item delete requests
     * @param deleteRequests Delete requests
     * @return this object with new state
     */
    public BatchDeleteRequest withDeleteRequests(List<DeleteRequest> deleteRequests) {
        this.deleteRequests = deleteRequests;
        return this;
    }

    /**
     * Sets the rate limiters for each type being deleted
     * @param rateLimiters Rate limiters
     * @return this object with new state
     */
    public BatchDeleteRequest withRateLimiters(Map<Class, DynamoRateLimiter> rateLimiters) {
        this.rateLimiters = rateLimiters;
        return this;
    }

    /**
     * @return list of the individual delete requests
     */
    public List<DeleteRequest> getDeleteRequests() {
        return deleteRequests;
    }

    /**
     * @return a map of the rate limiters for each type being deleted
     */
    public Map<Class, DynamoRateLimiter> getRateLimiters() {
        return rateLimiters;
    }
}

