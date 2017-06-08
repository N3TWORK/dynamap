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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.test.DummyDocBean;
import com.n3twork.dynamap.test.DummyDocUpdates;
import com.n3twork.dynamap.test.ExampleDocument;
import com.n3twork.dynamap.test.ExampleDocumentBean;
import com.n3twork.dynamap.test.ExampleDocumentUpdates;
import com.n3twork.dynamap.test.NestedTypeBean;
import com.n3twork.dynamap.test.NestedTypeUpdates;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class DynamapTest {

    private AmazonDynamoDB ddb;
    private Dynamap dynamap;
    private SchemaRegistry schemaRegistry;

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeTest
    public void init() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        ddb = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @BeforeMethod
    public void setup() {
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"));
        // Create tables
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(true);
    }

    @Test
    public void testDynaMap() throws Exception {
        // Save
        String exampleId = UUID.randomUUID().toString();
        String nestedId = UUID.randomUUID().toString();
        NestedTypeBean nestedObject = new NestedTypeBean(nestedId, null, null, null, null, null,
                null, null, null, null);
        ExampleDocumentBean doc = new ExampleDocumentBean(exampleId,
                1, nestedObject, null, null, "alias");
        dynamap.save(doc, null);

        ExampleDocumentBean doc2 = new ExampleDocumentBean(exampleId,
                2, nestedObject, null, null, "alias");

        // overwrite allowed
        dynamap.save(doc2, true, null);

        // overwrite will fail
        try {
            dynamap.save(doc2, false, null);
            Assert.fail();
        }
        catch (RuntimeException ex){
            Assert.assertNotNull(ex);
        }

        // Get Object
        GetObjectRequest<ExampleDocumentBean> getObjectRequest = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId).withRangeKeyValue(1);
        ExampleDocumentBean exampleDocument = dynamap.getObject(getObjectRequest, null);

        Assert.assertEquals(exampleDocument.getExampleId(), exampleId);
        nestedObject = new NestedTypeBean(exampleDocument.getNestedObject());
        Assert.assertEquals(nestedObject.getId(), nestedId);

        // Get Not Exists
        Assert.assertNull(dynamap.getObject(new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue("blah").withRangeKeyValue(1), null));

        // Update root object
        ExampleDocumentUpdates exampleDocumentUpdates = new ExampleDocumentUpdates(exampleDocument, exampleDocument.getHashKeyValue(), exampleDocument.getRangeKeyValue());
        exampleDocumentUpdates.setAlias("new alias");
        dynamap.update(exampleDocumentUpdates);

        exampleDocument = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocument.getAlias(), "new alias");


        // Update nested object
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(nestedObject, exampleId, 1);
        nestedTypeUpdates.setBio("test nested");
        dynamap.update(nestedTypeUpdates);

        exampleDocument = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocument.getNestedObject().getBio(), "test nested");


        // Update parent and nested object
        exampleDocumentUpdates = new ExampleDocumentUpdates(exampleDocument, exampleDocument.getHashKeyValue(), exampleDocument.getRangeKeyValue());
        exampleDocumentUpdates.setAlias("alias");
        nestedTypeUpdates.setBio("test");
        exampleDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(exampleDocumentUpdates);

        exampleDocument = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocument.getAlias(), "alias");
        Assert.assertEquals(exampleDocument.getNestedObject().getBio(), "test");

        // Query
        QueryRequest<ExampleDocumentBean> queryRequest = new QueryRequest<>(ExampleDocumentBean.class).withHashKeyValue("alias")
                .withRangeKeyCondition(new RangeKeyCondition("seq").eq(1)).withIndex(ExampleDocumentBean.GlobalSecondaryIndex.exampleIndex);
        List<ExampleDocumentBean> exampleDocuments = dynamap.query(queryRequest, null);
        Assert.assertEquals(exampleDocuments.size(), 1);
        Assert.assertEquals(exampleDocuments.get(0).getNestedObject().getBio(), "test");


        // Migration
        String jsonSchema = IOUtils.toString(getClass().getResourceAsStream("/TestSchema.json"));
        jsonSchema = jsonSchema.replace("\"version\": 1,", "\"version\": 2,");
        schemaRegistry = new SchemaRegistry(new ByteArrayInputStream(jsonSchema.getBytes()));
        schemaRegistry.registerMigration("Example", new Migration() {
            @Override
            public int getVersion() {
                return 2;
            }

            @Override
            public int getSequence() {
                return 0;
            }

            @Override
            public void migrate(Item item, int version, Object context) {
                item.withString("alias", "newAlias");
            }

            @Override
            public void postMigration(Item item, int version, Object context) {

            }
        });


        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        getObjectRequest = new GetObjectRequest(ExampleDocumentBean.class).withHashKeyValue(exampleId).withRangeKeyValue(1);
        exampleDocument = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocument.getAlias(), "newAlias");


        // Delete
        final int sequence = 3;
        ExampleDocumentBean doc3 = new ExampleDocumentBean(exampleId,
                sequence, nestedObject, null, null, "alias");
        dynamap.save(doc3, false, null);

        GetObjectRequest<ExampleDocumentBean> getObjectRequest3 = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId).withRangeKeyValue(sequence);
        ExampleDocument exampleDocument3 = dynamap.getObject(getObjectRequest3, null);
        Assert.assertNotNull(exampleDocument3);

        DeleteRequest<ExampleDocumentBean> deleteRequest = new DeleteRequest<>(ExampleDocumentBean.class)
                .withHashKeyValue(exampleId)
                .withRangeKeyValue(sequence);

        dynamap.delete(deleteRequest);

        getObjectRequest3 = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId).withRangeKeyValue(sequence);
        exampleDocument3 = dynamap.getObject(getObjectRequest3, null);
        Assert.assertNull(exampleDocument3);
    }

    @Test
    public void testOptimisticLocking() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean(DOC_ID, "test", 6);
        dynamap.save(doc, null);

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        DummyDocUpdates docUpdates = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        docUpdates.setWeight(100);
        dynamap.update(docUpdates);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);


        // two simultaneous updates, second one should fail
        DummyDocUpdates docUpdates1 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        DummyDocUpdates docUpdates2 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);

        dynamap.update(docUpdates1);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

        try {
            dynamap.update(docUpdates2);
            Assert.fail();
        }
        catch (RuntimeException ex){
            Assert.assertNotNull(ex);
            Assert.assertTrue(ex.getCause() instanceof com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException);
        }

        // optimist locking disabled
        DummyDocUpdates docUpdates3 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), false);
        DummyDocUpdates docUpdates4 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), false);
        dynamap.update(docUpdates3);
        dynamap.update(docUpdates4);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

    }


    @Test
    public void testOptimisticLockingWithSave() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean(DOC_ID, "test", 6);
        dynamap.save(doc, null);

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        savedDoc.setWeight(100);

        // two simultaneous updates, second one should fail
        dynamap.save(savedDoc, null);

        try {
            dynamap.save(savedDoc, null);
            Assert.fail();
        }
        catch (RuntimeException ex){
            Assert.assertNotNull(ex);
            Assert.assertTrue(ex.getCause() instanceof com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException);
        }

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        // two simultaneous updates, second one should fail
        dynamap.save(savedDoc, true, true, null);
        // two simultaneous updates, second one should fail
        dynamap.save(savedDoc, true, true, null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);
    }

    @Test
    public void testBatchSave() {
        batchSave(null);
    }

    @Test
    public void testBatchSaveWithRateLimiter() {
        Map<String, DynamoRateLimiter> limiterMap = new HashMap<>();
        limiterMap.put(ExampleDocumentBean.getTableName(), new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));
        limiterMap.put(DummyDocBean.getTableName(), new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));

        //TODO: need to do a better test the verifies correct limiter behavior
        batchSave(limiterMap);
    }

    private void batchSave(Map<String, DynamoRateLimiter> limiterMap) {
        final int EXAMPLE_DOCS_SIZE = 22;
        final int DUMMY_DOCS_SIZE = 23;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> exampleDocsIds = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String bigString = new String(new char[10000]).replace('\0', 'X');

        for (int i = 0; i < EXAMPLE_DOCS_SIZE; i++) {
            String exampleId = UUID.randomUUID().toString();
            String nestedId = UUID.randomUUID().toString();
            NestedTypeBean nestedObject = new NestedTypeBean(nestedId, null, null, null, null, null,
                    null, null, null, null);
            ExampleDocumentBean doc = new ExampleDocumentBean(exampleId,
                    1, nestedObject, null, null, "alias");

            exampleDocsIds.add(exampleId);
            docsToSave.add(doc);
        }

        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean(id, bigString, i);

            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        dynamap.batchSave(docsToSave, limiterMap);

        QueryRequest<ExampleDocumentBean> queryRequest = new QueryRequest<>(ExampleDocumentBean.class);
        List<ExampleDocumentBean> savedExampleDocs = dynamap.scan(queryRequest, null);

        QueryRequest<DummyDocBean> queryRequest2 = new QueryRequest<>(DummyDocBean.class);
        List<DummyDocBean> savedDummyDocs = dynamap.scan(queryRequest2, null);

        Assert.assertEquals(savedExampleDocs.size(), EXAMPLE_DOCS_SIZE);
        Assert.assertEquals(savedDummyDocs.size(), DUMMY_DOCS_SIZE);

        List<String> savedExampleDocsIds = savedExampleDocs.stream().map(ExampleDocumentBean::getExampleId).collect(Collectors.toList());
        List<String> savedDummyDocsIds = savedDummyDocs.stream().map(DummyDocBean::getId).collect(Collectors.toList());

        Assert.assertTrue(savedExampleDocsIds.containsAll(exampleDocsIds) && exampleDocsIds.containsAll(savedExampleDocsIds));
        Assert.assertTrue(savedDummyDocsIds.containsAll(dummyDocsIds) && dummyDocsIds.containsAll(savedDummyDocsIds));
    }
}
