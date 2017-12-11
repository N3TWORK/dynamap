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
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.n3twork.dynamap.test.*;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/ExampleSchema.json"),
                getClass().getResourceAsStream("/DummySchema.json"),
                getClass().getResourceAsStream("/DummyLocalIndexSchema.json"));
        // Create tables
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(true);
    }

    @Test
    public void testDynaMap() throws Exception {
        // Save
        String exampleId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setBio("biography");

        ExampleDocumentBean doc = new ExampleDocumentBean().setExampleId(exampleId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc, null);

        ExampleDocumentBean doc2 = new ExampleDocumentBean().setExampleId(exampleId1).setSequence(2).setNestedObject(nestedObject).setAlias("alias");

        // overwrite allowed
        dynamap.save(doc2, true, null);

        // overwrite will fail
        try {
            dynamap.save(doc2, false, null);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertNotNull(ex);
        }

        // Get Object
        GetObjectRequest<ExampleDocumentBean> getObjectRequest = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId1).withRangeKeyValue(1);
        ExampleDocumentBean exampleDocumentBean = dynamap.getObject(getObjectRequest, null);

        Assert.assertEquals(exampleDocumentBean.getExampleId(), exampleId1);
        nestedObject = new NestedTypeBean(exampleDocumentBean.getNestedObject());
        Assert.assertEquals(nestedObject.getId(), nestedId1);

        /// Test with rate limiters
        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        // Get Not Exists
        Assert.assertNull(dynamap.getObject(new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue("blah").withRangeKeyValue(1), rateLimiterPair, null));

        // Update root object
        ExampleDocumentUpdates exampleDocumentUpdates = new ExampleDocumentUpdates(exampleDocumentBean, exampleDocumentBean.getHashKeyValue(), exampleDocumentBean.getRangeKeyValue());
        exampleDocumentUpdates.setAlias("new alias");
        dynamap.update(exampleDocumentUpdates, rateLimiterPair.getWriteLimiter());

        exampleDocumentBean = dynamap.getObject(getObjectRequest, rateLimiterPair, null);
        Assert.assertEquals(exampleDocumentBean.getAlias(), "new alias");


        // Update nested object
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(nestedObject, exampleId1, 1);
        nestedTypeUpdates.setBio(null);
        dynamap.update(nestedTypeUpdates, rateLimiterPair.getWriteLimiter());

        exampleDocumentBean = dynamap.getObject(getObjectRequest, rateLimiterPair, null);
        Assert.assertNull(exampleDocumentBean.getNestedObject().getBio());


        // Update parent and nested object
        exampleDocumentUpdates = new ExampleDocumentUpdates(exampleDocumentBean, exampleDocumentBean.getHashKeyValue(), exampleDocumentBean.getRangeKeyValue());
        exampleDocumentUpdates.setAlias("alias");
        nestedTypeUpdates = new NestedTypeUpdates(exampleDocumentBean.getNestedObject(), exampleId1, 1);
        nestedTypeUpdates.setBio("test");
        exampleDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(exampleDocumentUpdates, rateLimiterPair.getWriteLimiter());

        exampleDocumentBean = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocumentBean.getAlias(), "alias");
        Assert.assertEquals(exampleDocumentBean.getNestedObject().getBio(), "test");

        // Map of Custom Object
        exampleDocumentUpdates = new ExampleDocumentUpdates(exampleDocumentBean, exampleDocumentBean.getHashKeyValue(), exampleDocumentBean.getRangeKeyValue());
        exampleDocumentUpdates.setMapOfCustomTypeItem("test", new CustomType("test", "test", CustomType.CustomTypeEnum.VALUE_A));
        dynamap.update(exampleDocumentUpdates, null);
        exampleDocumentBean = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocumentBean.getMapOfCustomTypeItem("test").getName(), "test");
        // Test delete without using current state
        exampleDocumentUpdates = new ExampleDocumentUpdates(new ExampleDocumentBean(), exampleDocumentBean.getHashKeyValue(), exampleDocumentBean.getRangeKeyValue());
        exampleDocumentUpdates.deleteMapOfCustomTypeItem("test");
        ExampleDocument exampleDocument = dynamap.update(exampleDocumentUpdates, null);
        Assert.assertFalse(exampleDocument.getMapOfCustomTypeIds().contains("test"));


        // Query
        QueryRequest<ExampleDocumentBean> queryRequest = new QueryRequest<>(ExampleDocumentBean.class).withHashKeyValue("alias")
                .withRangeKeyCondition(new RangeKeyCondition("seq").eq(1)).withIndex(ExampleDocumentBean.GlobalSecondaryIndex.exampleIndex);
        List<ExampleDocumentBean> exampleDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(exampleDocuments.size(), 1);
        Assert.assertEquals(exampleDocuments.get(0).getNestedObject().getBio(), "test");


        // Migration
        String jsonSchema = IOUtils.toString(getClass().getResourceAsStream("/ExampleSchema.json"));
        jsonSchema = jsonSchema.replace("\"version\": 1,", "\"version\": 2,");
        schemaRegistry = new SchemaRegistry(new ByteArrayInputStream(jsonSchema.getBytes()));
        schemaRegistry.registerMigration(ExampleDocumentBean.class, new Migration() {
            @Override
            public int getVersion() {
                return 2;
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
        getObjectRequest = new GetObjectRequest(ExampleDocumentBean.class).withHashKeyValue(exampleId1).withRangeKeyValue(1);
        exampleDocumentBean = dynamap.getObject(getObjectRequest, null);
        Assert.assertEquals(exampleDocumentBean.getAlias(), "newAlias");


        // Delete
        final int sequence = 3;
        ExampleDocumentBean doc3 = new ExampleDocumentBean().setExampleId(exampleId1).setSequence(sequence).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc3, false, null);

        GetObjectRequest<ExampleDocumentBean> getObjectRequest3 = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId1).withRangeKeyValue(sequence);
        ExampleDocument exampleDocument3 = dynamap.getObject(getObjectRequest3, null);
        Assert.assertNotNull(exampleDocument3);

        DeleteRequest<ExampleDocumentBean> deleteRequest = new DeleteRequest<>(ExampleDocumentBean.class)
                .withHashKeyValue(exampleId1)
                .withRangeKeyValue(sequence);

        dynamap.delete(deleteRequest);

        getObjectRequest3 = new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId1).withRangeKeyValue(sequence);
        exampleDocument3 = dynamap.getObject(getObjectRequest3, null);
        Assert.assertNull(exampleDocument3);
    }

    @Test
    public void testBatchGetItem() {
        String exampleId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();
        String exampleId2 = UUID.randomUUID().toString();
        String nestedId2 = UUID.randomUUID().toString();
        dynamap.save(new ExampleDocumentBean().setExampleId(exampleId1).setSequence(1).setAlias("alias")
                .setNestedObject(new NestedTypeBean().setId(nestedId1)), null);
        dynamap.save(new ExampleDocumentBean().setExampleId(exampleId2).setSequence(1).setAlias("alias")
                .setNestedObject(new NestedTypeBean().setId(nestedId2)), null);


        List<ExampleDocumentBean> exampleDocuments;

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        BatchGetObjectRequest<ExampleDocumentBean> batchGetObjectRequest = new BatchGetObjectRequest()
                .withGetObjectRequests(ImmutableList.of(
                        new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId1).withRangeKeyValue(1),
                        new GetObjectRequest<>(ExampleDocumentBean.class).withHashKeyValue(exampleId2).withRangeKeyValue(1)))
                .withRateLimiters(rateLimiterPair);

        exampleDocuments = dynamap.batchGetObjectSingleCollection(batchGetObjectRequest);

        Assert.assertEquals(exampleDocuments.size(), 2);
    }

    @Test
    public void testOptimisticLocking() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean().setId(DOC_ID).setName("test").setWeight(6);
        dynamap.save(doc, null);

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        DummyDocUpdates docUpdates = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());
        docUpdates.setWeight(100);
        dynamap.update(docUpdates, null);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);


        // two simultaneous updates, second one should fail
        DummyDocUpdates docUpdates1 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());
        DummyDocUpdates docUpdates2 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());

        dynamap.update(docUpdates1, null);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

        try {
            dynamap.update(docUpdates2, null);
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
        }

        // optimist locking disabled
        DummyDocUpdates docUpdates3 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        DummyDocUpdates docUpdates4 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        dynamap.update(docUpdates3, null);
        dynamap.update(docUpdates4, null);

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

    }

    @Test
    public void testConditionalChecks() {

        String exampleId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setMapOfLong(ImmutableMap.of("dollars", 1L, "francs", 1L));

        ExampleDocumentBean doc = new ExampleDocumentBean().setExampleId(exampleId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc, null);

        // add 1 to dollars and a check to ensure it is less than 2
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), exampleId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(ExampleDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        dynamap.update(nestedTypeUpdates, null);
        // increment again and check that conditional exception is thrown
        nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), exampleId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(ExampleDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        boolean errorThrown = false;
        try {
            dynamap.update(nestedTypeUpdates, null);
        } catch (ConditionalCheckFailedException e) {
            errorThrown = true;
        }
        Assert.assertTrue(errorThrown);


    }

    @Test
    public void testOptimisticLockingWithSave() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean().setId(DOC_ID).setName("test").setWeight(6);
        dynamap.save(doc, null);

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        savedDoc.setWeight(100);

        // two simultaneous updates, second one should fail
        dynamap.save(savedDoc, null);

        try {
            dynamap.save(savedDoc, null);
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
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
        Map<Class, DynamoRateLimiter> limiterMap = new HashMap<>();
        limiterMap.put(ExampleDocumentBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));
        limiterMap.put(DummyDocBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));

        //TODO: need to do a better test the verifies correct limiter behavior
        batchSave(limiterMap);
    }

    private void batchSave(Map<Class, DynamoRateLimiter> limiterMap) {
        final int EXAMPLE_DOCS_SIZE = 22;
        final int DUMMY_DOCS_SIZE = 23;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> exampleDocsIds = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String bigString = new String(new char[10000]).replace('\0', 'X');

        for (int i = 0; i < EXAMPLE_DOCS_SIZE; i++) {
            String exampleId = UUID.randomUUID().toString();
            String nestedId = UUID.randomUUID().toString();
            NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId);
            ExampleDocumentBean doc = new ExampleDocumentBean().setExampleId(exampleId).setSequence(1).setNestedObject(nestedObject).setAlias("alias");

            exampleDocsIds.add(exampleId);
            docsToSave.add(doc);
        }

        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean().setId(id).setName(bigString).setWeight(i);

            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        dynamap.batchSave(docsToSave, limiterMap);

        ScanRequest<ExampleDocumentBean> scanRequest = new ScanRequest<>(ExampleDocumentBean.class);
        ScanResult<ExampleDocumentBean> scanResult = dynamap.scan(scanRequest);
        List<ExampleDocumentBean> savedExampleDocs = scanResult.getResults();

        ScanRequest<DummyDocBean> scanRequest2 = new ScanRequest<>(DummyDocBean.class);
        ScanResult<DummyDocBean> scanResult2 = dynamap.scan(scanRequest2);
        List<DummyDocBean> savedDummyDocs = scanResult2.getResults();

        Assert.assertEquals(savedExampleDocs.size(), EXAMPLE_DOCS_SIZE);
        Assert.assertEquals(savedDummyDocs.size(), DUMMY_DOCS_SIZE);

        List<String> savedExampleDocsIds = savedExampleDocs.stream().map(ExampleDocumentBean::getExampleId).collect(Collectors.toList());
        List<String> savedDummyDocsIds = savedDummyDocs.stream().map(DummyDocBean::getId).collect(Collectors.toList());

        Assert.assertTrue(savedExampleDocsIds.containsAll(exampleDocsIds) && exampleDocsIds.containsAll(savedExampleDocsIds));
        Assert.assertTrue(savedDummyDocsIds.containsAll(dummyDocsIds) && dummyDocsIds.containsAll(savedDummyDocsIds));
    }

    @Test
    public void testBeanSubclass() {
        String exampleId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setBio("biography");

        ExampleDocumentBean doc = new ExampleDocumentBean().setExampleId(exampleId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc, null);

        GetObjectRequest<ExampleDocumentBeanSubclass> getObjectRequest = new GetObjectRequest<>(ExampleDocumentBeanSubclass.class).withHashKeyValue(exampleId1).withRangeKeyValue(1);
        ExampleDocumentBeanSubclass exampleDocumentBean = dynamap.getObject(getObjectRequest, null);

        Assert.assertNotNull(exampleDocumentBean.getExampleId(), exampleId1);
        nestedObject = new NestedTypeBean(exampleDocumentBean.getNestedObject());
        Assert.assertEquals(nestedObject.getId(), nestedId1);
    }

    @Test
    public void testMultipleSuffix() {
        final int MAX = 10;
        IntStream.range(0, MAX).forEach(i ->  dynamap.createTableFromExisting(DummyDocBean.getTableName(), DummyDocBean.getTableName() + "-"  + i, true));

        // save and get
        IntStream.range(0, MAX).forEach(i -> {
            String suffix = "-" + i;
            DummyDocBean doc = new DummyDocBean().setId(Integer.toString(i)).setName("test"+i).setWeight(RandomUtils.nextInt(0,100));
            dynamap.save(doc, true, false, null, suffix);
            doc = new DummyDocBean().setId(Integer.toString(i+1000)).setName("test"+i).setWeight(RandomUtils.nextInt(0,100));
            dynamap.save(doc, true, false, null, suffix);
            DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i)).withSuffix(suffix), null);
            Assert.assertEquals(savedDoc.getId(), Integer.toString(i));
        });

        // batch get
        List<GetObjectRequest<DummyDocBean>> getObjectRequests = IntStream.range(0, MAX).mapToObj(i -> {
            String suffix = "-" + i;
            return new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i + 1000)).withSuffix(suffix);
        }).collect(Collectors.toList());

        BatchGetObjectRequest<DummyDocBean> batchRequest = new BatchGetObjectRequest().withGetObjectRequests(getObjectRequests);
        List<DummyDocBean> dummyDocBeans = dynamap.batchGetObjectSingleCollection(batchRequest);
        Assert.assertEquals(dummyDocBeans.size(), MAX);

        // updates
        DummyDocBean dummyDocBean = dummyDocBeans.get(0);
        String docId = dummyDocBean.getId();

        DummyDocUpdates docUpdates = new DummyDocUpdates(dummyDocBean, dummyDocBean.getHashKeyValue());
        String updatedName = "updated name";
        docUpdates.setName(updatedName);
        // deduct table suffix by the id
        String suffix = "-" + (Integer.valueOf(docId) % 10);
        dynamap.update(docUpdates, null, suffix);

        DummyDocBean updatedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix), null);
        Assert.assertEquals(updatedDoc.getId(), docId);

        //batch save
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        final int DUMMY_DOCS_SIZE = 9;
        List<String> dummyDocsIds = new ArrayList<>();
        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean().setId(id).setName("name" + i).setWeight(i);

            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        String suffix2 = "-10";
        dynamap.createTableFromExisting(DummyDocBean.getTableName(), DummyDocBean.getTableName() + suffix2, true);
        dynamap.batchSave(docsToSave, null, suffix2);

        // scan
        ScanRequest<DummyDocBean> scanRequest = new ScanRequest<>(DummyDocBean.class).withSuffix(suffix2);
        ScanResult<DummyDocBean> scanResult= dynamap.scan(scanRequest);
        List<DummyDocBean> savedDummyDocs = scanResult.getResults();
        List<String> savedDummyDocsIds = savedDummyDocs.stream().map(DummyDocBean::getId).collect(Collectors.toList());
        Assert.assertEquals(savedDummyDocs.size(), DUMMY_DOCS_SIZE);
        Assert.assertTrue(savedDummyDocsIds.containsAll(dummyDocsIds) && dummyDocsIds.containsAll(savedDummyDocsIds));

        // delete
        DummyDocBean doc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix), null);
        Assert.assertNotNull(doc);
        DeleteRequest<DummyDocBean> deleteRequest = new DeleteRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix);
        dynamap.delete(deleteRequest);
        doc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix), null);
        Assert.assertNull(doc);
    }

    @Test
    public void testLocalSecondaryIndex() {
        final int DUMMY_DOCS_SIZE = 13;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String id = "123";
        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            DummyDoc2Bean doc = new DummyDoc2Bean().setId(id).setName("name" + i).setWeight(i);
            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        dynamap.batchSave(docsToSave, null);

        QueryRequest<DummyDoc2Bean> queryRequest = new QueryRequest<>(DummyDoc2Bean.class)
                .withHashKeyValue(id)
                .withRangeKeyCondition(new RangeKeyCondition(DummyDocBean.WEIGHT_FIELD).eq(0))
                .withIndex(DummyDoc2Bean.LocalSecondaryIndex.weightIndex);

        List<DummyDoc2Bean> docs = dynamap.query(queryRequest);
        Assert.assertEquals(docs.size(), 1);

    }
}
