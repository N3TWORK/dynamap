package com.n3twork.dynamap;


import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DynamapCreateTableTest {
    private final static ObjectMapper objectMapper = new ObjectMapper();

    private DynamoDbClient ddb;
    private Dynamap dynamap;

    @BeforeClass
    public void setUpClass() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        ddb = DynamoDbClient.builder().build();
    }

    @BeforeMethod
    public void setUp() {
        SchemaRegistry schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"));
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("create-table-test.").withObjectMapper(objectMapper);
    }

    @Test
    public void testCreateProvisionedTable() {
        dynamap.createTables(true, 10, 11);
        TableDescription description = ddb.describeTable(DescribeTableRequest.builder().tableName("create-table-test.Test").build()).table();
        assertEquals((long) description.provisionedThroughput().readCapacityUnits(), 10);
        assertEquals((long) description.provisionedThroughput().writeCapacityUnits(), 11);
    }

    @Test
    public void testCreatePayPerRequestTable() {
        dynamap.createTables(true, request -> {
            CreateTableRequest builder = request.toBuilder()
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .provisionedThroughput(null);

            if (request.globalSecondaryIndexes() != null) {
                for (GlobalSecondaryIndex gsi : request.globalSecondaryIndexes()) {
                    gsi.provisionedThroughput(null);
                }
            }
        });
        TableDescription description = ddb.describeTable(DescribeTableRequest.builder().tableName("create-table-test.Test").build()).table();
        assertEquals(description.billingModeSummary().billingMode(), BillingMode.PAY_PER_REQUEST);
    }
}
