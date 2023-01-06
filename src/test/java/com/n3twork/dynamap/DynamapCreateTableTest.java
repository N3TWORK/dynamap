package com.n3twork.dynamap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DynamapCreateTableTest {
    private final static ObjectMapper objectMapper = new ObjectMapper();

    private AmazonDynamoDB ddb;
    private Dynamap dynamap;

    @BeforeClass
    public void setUpClass() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        ddb = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @BeforeMethod
    public void setUp() {
        SchemaRegistry schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"));
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("create-table-test.").withObjectMapper(objectMapper);
    }

    @Test
    public void testCreateProvisionedTable() {
        dynamap.createTables(true, 10, 11);
        TableDescription description = ddb.describeTable(new DescribeTableRequest().withTableName("create-table-test.Test")).getTable();
        assertEquals((long) description.getProvisionedThroughput().getReadCapacityUnits(), 10);
        assertEquals((long) description.getProvisionedThroughput().getWriteCapacityUnits(), 11);
    }

    @Test
    public void testCreatePayPerRequestTable() {
        dynamap.createTables(true, request -> {
            request
                    .withBillingMode(BillingMode.PAY_PER_REQUEST)
                    .withProvisionedThroughput(null);

            if (request.getGlobalSecondaryIndexes() != null) {
                for (GlobalSecondaryIndex gsi : request.getGlobalSecondaryIndexes()) {
                    gsi.withProvisionedThroughput(null);
                }
            }
        });
        TableDescription description = ddb.describeTable(new DescribeTableRequest().withTableName("create-table-test.Test")).getTable();
        assertEquals(description.getBillingModeSummary().getBillingMode(), "PAY_PER_REQUEST");
    }
}
