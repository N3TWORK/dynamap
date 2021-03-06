/*
    Copyright 2020 N3TWORK INC

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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DynamapTtlTest {
    private AmazonDynamoDB ddb;
    private Dynamap dynamap;
    private SchemaRegistry schemaRegistry;

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeTest
    public void init() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        ddb = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @Test
    public void ttlShouldBeSetOnNewTables() {
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchemaWithTtlA.json"));
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(System.getProperty("aws.profile") == null);

        DescribeTimeToLiveResult describeTimeToLiveResult = ddb.describeTimeToLive(new DescribeTimeToLiveRequest().withTableName("testTestWithTtl"));
        // In a scenario with a live DynamoDB connection, it will take some time for the change to apply. But with DynamoDBLocal it happens synchronously.
        Assert.assertEquals(describeTimeToLiveResult.getTimeToLiveDescription(), new TimeToLiveDescription().withAttributeName("ttlA").withTimeToLiveStatus(TimeToLiveStatus.ENABLED));
    }

    @Test(dependsOnMethods = {"ttlShouldBeSetOnNewTables"})
    public void ttlShouldBeSetOnExistingTables() {
        // Disable the current TTL. This is required by DynamoDB.
        ddb.updateTimeToLive(new UpdateTimeToLiveRequest().withTableName("testTestWithTtl").withTimeToLiveSpecification(new TimeToLiveSpecification().withAttributeName("ttlA").withEnabled(false)));
        DescribeTimeToLiveResult describeTimeToLiveResult = ddb.describeTimeToLive(new DescribeTimeToLiveRequest().withTableName("testTestWithTtl"));
        // In a scenario with a live DynamoDB connection, it will take some time for the change to apply. But with DynamoDBLocal it happens synchronously.
        Assert.assertEquals(describeTimeToLiveResult.getTimeToLiveDescription(), new TimeToLiveDescription().withTimeToLiveStatus(TimeToLiveStatus.DISABLED));

        // Call createTables with an updated schema specifying `ttlB` as the table TTL field.
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchemaWithTtlB.json")); // Same table except for the new TTL field.
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(System.getProperty("aws.profile") == null);

        describeTimeToLiveResult = ddb.describeTimeToLive(new DescribeTimeToLiveRequest().withTableName("testTestWithTtl"));
        // In a scenario with a live DynamoDB connection, it will take some time for the change to apply. But with DynamoDBLocal it happens synchronously.
        Assert.assertEquals(describeTimeToLiveResult.getTimeToLiveDescription(), new TimeToLiveDescription().withAttributeName("ttlB").withTimeToLiveStatus(TimeToLiveStatus.ENABLED));
    }
}
