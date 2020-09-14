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

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.model.TableDefinition;
import com.n3twork.dynamap.test.PlayerBean;
import com.n3twork.dynamap.test.PlayerUpdates;
import com.n3twork.dynamap.tx.ReadTx;
import com.n3twork.dynamap.tx.WriteTx;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

public class DynamapTxTest {

    private AmazonDynamoDB ddb;
    private Dynamap dynamap;
    private SchemaRegistry schemaRegistry;

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeTest
    public void init() {

        // This test can be run against an AWS account.
        // This is necessary for testing the UpdateResult behavior because currently local dynamodb does not correctly implement UPDATE_NEW return values.
        if (System.getProperty("aws.profile") != null) {
            AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClient.builder();
            builder.setCredentials(new ProfileCredentialsProvider((System.getProperty("aws.profile"))));
            builder.setRegion("us-east-1");
            ddb = builder.build();
        } else {
            System.setProperty("sqlite4java.library.path", "native-libs");
            ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        }
    }

    @BeforeMethod
    public void setup() {
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/PlayerSchema.json"));
        // Create tables
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(System.getProperty("aws.profile") == null);
    }

    @Test
    public void testWriteTx() {
        PlayerBean p1 = new PlayerBean("playerOne", "Player One", PlayerBean.SCHEMA_VERSION);
        PlayerBean p2 = new PlayerBean("playerTwo", "Player Two", PlayerBean.SCHEMA_VERSION);
        TableDefinition playerTableDef = schemaRegistry.getTableDefinition(PlayerBean.class);

        // Create two players in a single transaction.
        WriteTx createTwoPlayers = dynamap.newWriteTx();
        createTwoPlayers.put(p1);
        createTwoPlayers.put(p2);
        createTwoPlayers.exec();

        PlayerBean playerOneRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerOne")));
        assertNotNull(playerOneRead);
        assertEquals(playerOneRead, p1);

        PlayerBean playerTwoRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerTwo")));
        assertNotNull(playerTwoRead);
        assertEquals(playerTwoRead, p2);

        // Update two players in a single transaction.
        PlayerUpdates playerOneUpdates = playerOneRead.createUpdates();
        playerOneUpdates.setName("I am Player One");
        PlayerUpdates playerTwoUpdates = playerTwoRead.createUpdates();
        playerTwoUpdates.setName("I am Player Two");
        WriteTx updateTwoPlayers = dynamap.newWriteTx();
        updateTwoPlayers.update(new UpdateParams<>(playerOneUpdates));
        updateTwoPlayers.update(new UpdateParams<>(playerTwoUpdates));
        updateTwoPlayers.exec();

        playerOneRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerOne")));
        assertNotNull(playerOneRead);
        assertEquals(playerOneRead.getName(), "I am Player One");

        playerTwoRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerTwo")));
        assertNotNull(playerTwoRead);
        assertEquals(playerTwoRead.getName(), "I am Player Two");

        // Delete two players in a single transaction.
        WriteTx deleteTwoPlayers = dynamap.newWriteTx();
        deleteTwoPlayers.delete(new DeleteRequest(PlayerBean.class).withHashKeyValue("playerOne"));
        deleteTwoPlayers.delete(new DeleteRequest(PlayerBean.class).withHashKeyValue("playerTwo"));
        deleteTwoPlayers.exec();

        playerOneRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerOne")));
        assertNull(playerOneRead);

        playerTwoRead = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerTwo")));
        assertNull(playerTwoRead);
    }

    @Test
    public void testReadTx() {
        PlayerBean p1 = new PlayerBean("playerOne", "Player One", PlayerBean.SCHEMA_VERSION);
        PlayerBean p2 = new PlayerBean("playerTwo", "Player Two", PlayerBean.SCHEMA_VERSION);
        dynamap.save(new SaveParams(p1));
        dynamap.save(new SaveParams(p2));

        ReadTx readTx = dynamap.newReadTx();
        readTx.get(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerOne")));
        readTx.get(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("i_do_not_exist")));
        readTx.get(new GetObjectParams<>(new GetObjectRequest<>(PlayerBean.class).withHashKeyValue("playerTwo")));
        List<DynamapRecordBean> result = readTx.exec();

        assertEquals(result.size(), 3);
        assertEquals(result.get(0), p1);
        assertNull(result.get(1));
        assertEquals(result.get(2), p2);
    }
}