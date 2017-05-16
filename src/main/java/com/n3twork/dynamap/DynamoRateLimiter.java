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

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.google.common.util.concurrent.RateLimiter;

import java.util.List;
import java.util.stream.Collectors;

public class DynamoRateLimiter {

    public static boolean DISABLE_RATE_LIMITING = false; // For integration tests

    private RateLimiter rateLimiter;
    private int permitsToConsume = 1;
    private final RateLimitType rateLimitType;
    private final int targetPercent;

    public enum RateLimitType {READ, WRITE}

    public DynamoRateLimiter(RateLimitType rateLimitType, int targetPercent) {
        this.rateLimitType = rateLimitType;
        this.targetPercent = targetPercent;
    }

    public void init(Table table) {
        init(table, null);
    }

    public void init(Table table, String indexName) {
        if (rateLimiter == null) {
            table.describe();
            if (table.getDescription() != null) {
                ProvisionedThroughputDescription provisionedThroughputDescription;
                if (indexName != null) {
                    provisionedThroughputDescription = table.getDescription().getGlobalSecondaryIndexes()
                            .stream().filter(i -> i.getIndexName().equals(indexName)).findFirst().get().getProvisionedThroughput();
                } else {
                    provisionedThroughputDescription = table.getDescription().getProvisionedThroughput();
                }
                if (RateLimitType.READ.equals(rateLimitType)) {
                    double permitsPerSec = provisionedThroughputDescription.getReadCapacityUnits() / (100 / targetPercent);
                    rateLimiter = RateLimiter.create(Math.max(1, permitsPerSec)); // units per second
                } else {
                    double permitsPerSec = provisionedThroughputDescription.getWriteCapacityUnits() / (100 / targetPercent);
                    rateLimiter = RateLimiter.create(Math.max(1, permitsPerSec)); // units per second
                }
            }
        }
    }

    public void acquire() {
        if (rateLimiter != null) {
            if (!DISABLE_RATE_LIMITING) {
                rateLimiter.acquire(permitsToConsume);
            }
        } else {
            throw new RuntimeException("Not initialized");
        }
    }

    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        if (consumedCapacity != null) {
            permitsToConsume = (int) (consumedCapacity.getCapacityUnits() - 1.0);
            if (permitsToConsume <= 0) {
                permitsToConsume = 1;
            }
        }
    }

    public void setConsumedCapacity(List<ConsumedCapacity> consumedCapacities) {
        if (consumedCapacities != null) {
            double totalUnits = consumedCapacities.stream().collect(Collectors.summingDouble(c -> c.getCapacityUnits()));
            permitsToConsume = (int) (totalUnits - 1.0);
            if (permitsToConsume <= 0) {
                permitsToConsume = 1;
            }
        }
    }

}
