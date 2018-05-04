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
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"),
                getClass().getResourceAsStream("/DummySchema.json"),
                getClass().getResourceAsStream("/DummyLocalIndexSchema.json"));
        // Create tables
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(true);
    }

    @Test
    public void testGetObject() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(doc, null);

        // Get Object
        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(testDocumentBean.getTestId(), doc.getTestId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        // Get Not Exists
        Assert.assertNull(dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue("blah").withRangeKeyValue(1), null));

    }

    @Test
    public void testStringField() {

        NestedTypeBean nested = createNestedTypeBean();
        nested.setBio("bio");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setAlias("alias");
        dynamap.save(doc, null);

        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(testDocumentBean.getTestId(), doc.getTestId());
        Assert.assertEquals(testDocumentBean.getAlias(), doc.getAlias());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getBio(), nested.getBio());
    }

    @Test
    public void testRateLimiters() {

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        NestedTypeBean nested = createNestedTypeBean();
        nested.setBio("bio");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setAlias("alias");
        dynamap.save(doc, rateLimiterPair.getWriteLimiter());

        dynamap.getObject(createGetObjectRequest(doc), rateLimiterPair, null);

        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.setAlias("new alias");
        dynamap.update(testDocumentUpdates, rateLimiterPair.getWriteLimiter());

    }

    @Test
    public void updateRootAndNested() {

        NestedTypeBean nested = createNestedTypeBean();
        nested.setBio("bio");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setAlias("alias");
        dynamap.save(doc, null);

        // Update parent and nested object
        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.setAlias("alias2");
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(doc.getNestedObject(), doc.getTestId(), doc.getRangeKeyValue());
        nestedTypeUpdates.setBio("bio2");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(testDocumentUpdates, null);

        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getAlias(), "alias2");
        Assert.assertEquals(doc.getNestedObject().getBio(), "bio2");
    }

    @Test
    public void testMapOfCustomObject() {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(doc, null);

        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.setMapOfCustomTypeItem("test", new CustomType("test", "test", CustomType.CustomTypeEnum.VALUE_A));
        dynamap.update(testDocumentUpdates, null);
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getMapOfCustomTypeItem("test").getName(), "test");
        // Test delete without using current state
        testDocumentUpdates = new TestDocumentUpdates(new TestDocumentBean(), doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.deleteMapOfCustomTypeItem("test");
        TestDocument testDocument = dynamap.update(testDocumentUpdates, null);
        Assert.assertFalse(testDocument.getMapOfCustomTypeIds().contains("test"));
    }

    @Test
    public void testQuery() {

        NestedTypeBean nestedTypeBean = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(doc, null);

        QueryRequest<TestDocumentBean> queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue("alias")
                .withRangeKeyCondition(new RangeKeyCondition("seq").eq(doc.getSequence())).withIndex(TestDocumentBean.GlobalSecondaryIndex.testIndex);
        List<TestDocumentBean> testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getBio(), nestedTypeBean.getBio());
    }

    @Test
    public void testMigration() throws Exception {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        doc.setAlias("alias");
        dynamap.save(doc, null);

        String jsonSchema = IOUtils.toString(getClass().getResourceAsStream("/TestSchema.json"));
        jsonSchema = jsonSchema.replace("\"version\": 1,", "\"version\": 2,");
        schemaRegistry = new SchemaRegistry(new ByteArrayInputStream(jsonSchema.getBytes()));
        schemaRegistry.registerMigration(TestDocumentBean.class, new Migration() {
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
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getAlias(), "newAlias");
    }

    @Test
    public void testDelete() {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(doc, null);

        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNotNull(doc);

        DeleteRequest<TestDocumentBean> deleteRequest = new DeleteRequest<>(TestDocumentBean.class)
                .withHashKeyValue(doc.getTestId())
                .withRangeKeyValue(doc.getRangeKeyValue());

        dynamap.delete(deleteRequest);
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNull(doc);
    }

    @Test
    public void testOverwrite() {
        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(doc, null);

        // overwrite allowed
        TestDocumentBean doc2 = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(doc2, true, null);

        // overwrite will fail
        try {
            dynamap.save(doc2, false, null);
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertNotNull(ex);
        }
    }

    @Test
    public void testIncrementAndSetMapOfLong() {
        String testId1 = UUID.randomUUID().toString();
        Map<String, Long> mapOfLong = new HashMap<>();
        mapOfLong.put("a", 1L);

        TestDocumentBean doc = new TestDocumentBean().setTestId(testId1).setSequence(1).setMapOfLong(mapOfLong);
        dynamap.save(doc, null);

        doc = dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testId1).withRangeKeyValue(1), null);
        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, testId1, 1);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 2);
        dynamap.update(testDocumentUpdates, null);
        doc = dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testId1).withRangeKeyValue(1), null);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 2);

        testDocumentUpdates = new TestDocumentUpdates(doc, testId1, 1);
        testDocumentUpdates.setMapOfLongValue("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        dynamap.update(testDocumentUpdates, null);
        doc = dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testId1).withRangeKeyValue(1), null);
        Assert.assertEquals(doc.getMapOfLongValue("a").longValue(), 1);

        // Set value then increment - only the set value is considered. Once set value is used, deltas are ignored.
        testDocumentUpdates = new TestDocumentUpdates(doc, testId1, 1);
        testDocumentUpdates.setMapOfLongValue("a", 10L);
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 10L);

    }


    @Test
    public void testBatchGetItem() {
        String testId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();
        String testId2 = UUID.randomUUID().toString();
        String nestedId2 = UUID.randomUUID().toString();
        dynamap.save(new TestDocumentBean().setTestId(testId1).setSequence(1).setAlias("alias")
                .setNestedObject(new NestedTypeBean().setId(nestedId1)), null);
        dynamap.save(new TestDocumentBean().setTestId(testId2).setSequence(1).setAlias("alias")
                .setNestedObject(new NestedTypeBean().setId(nestedId2)), null);


        List<TestDocumentBean> testDocuments;

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        BatchGetObjectRequest<TestDocumentBean> batchGetObjectRequest = new BatchGetObjectRequest()
                .withGetObjectRequests(ImmutableList.of(
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testId1).withRangeKeyValue(1),
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testId2).withRangeKeyValue(1)))
                .withRateLimiters(rateLimiterPair);

        testDocuments = dynamap.batchGetObjectSingleCollection(batchGetObjectRequest);

        Assert.assertEquals(testDocuments.size(), 2);
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

        String testId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setMapOfLong(ImmutableMap.of("dollars", 1L, "francs", 1L));

        TestDocumentBean doc = new TestDocumentBean().setTestId(testId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc, null);

        // add 1 to dollars and a check to ensure it is less than 2
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), testId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        dynamap.update(nestedTypeUpdates, null);
        // increment again and check that conditional exception is thrown
        nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), testId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
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
        limiterMap.put(TestDocumentBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));
        limiterMap.put(DummyDocBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));

        //TODO: need to do a better test the verifies correct limiter behavior
        batchSave(limiterMap);
    }

    private void batchSave(Map<Class, DynamoRateLimiter> limiterMap) {
        final int EXAMPLE_DOCS_SIZE = 22;
        final int DUMMY_DOCS_SIZE = 23;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> testDocsIds = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String bigString = new String(new char[10000]).replace('\0', 'X');

        for (int i = 0; i < EXAMPLE_DOCS_SIZE; i++) {
            String testId = UUID.randomUUID().toString();
            String nestedId = UUID.randomUUID().toString();
            NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId);
            TestDocumentBean doc = new TestDocumentBean().setTestId(testId).setSequence(1).setNestedObject(nestedObject).setAlias("alias");

            testDocsIds.add(testId);
            docsToSave.add(doc);
        }

        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean().setId(id).setName(bigString).setWeight(i);

            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        dynamap.batchSave(docsToSave, limiterMap);

        ScanRequest<TestDocumentBean> scanRequest = new ScanRequest<>(TestDocumentBean.class);
        ScanResult<TestDocumentBean> scanResult = dynamap.scan(scanRequest);
        List<TestDocumentBean> savedTestDocs = scanResult.getResults();

        ScanRequest<DummyDocBean> scanRequest2 = new ScanRequest<>(DummyDocBean.class);
        ScanResult<DummyDocBean> scanResult2 = dynamap.scan(scanRequest2);
        List<DummyDocBean> savedDummyDocs = scanResult2.getResults();

        Assert.assertEquals(savedTestDocs.size(), EXAMPLE_DOCS_SIZE);
        Assert.assertEquals(savedDummyDocs.size(), DUMMY_DOCS_SIZE);

        List<String> savedTestDocsIds = savedTestDocs.stream().map(TestDocumentBean::getTestId).collect(Collectors.toList());
        List<String> savedDummyDocsIds = savedDummyDocs.stream().map(DummyDocBean::getId).collect(Collectors.toList());

        Assert.assertTrue(savedTestDocsIds.containsAll(testDocsIds) && testDocsIds.containsAll(savedTestDocsIds));
        Assert.assertTrue(savedDummyDocsIds.containsAll(dummyDocsIds) && dummyDocsIds.containsAll(savedDummyDocsIds));
    }

    @Test
    public void testBeanSubclass() {
        String testId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setBio("biography");

        TestDocumentBean doc = new TestDocumentBean().setTestId(testId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setAlias("alias");
        dynamap.save(doc, null);

        GetObjectRequest<TestDocumentBeanSubclass> getObjectRequest = new GetObjectRequest<>(TestDocumentBeanSubclass.class).withHashKeyValue(testId1).withRangeKeyValue(1);
        TestDocumentBeanSubclass testDocumentBean = dynamap.getObject(getObjectRequest, null);

        Assert.assertNotNull(testDocumentBean.getTestId(), testId1);
        nestedObject = new NestedTypeBean(testDocumentBean.getNestedObject());
        Assert.assertEquals(nestedObject.getId(), nestedId1);
    }

    @Test
    public void testMultipleSuffix() {
        final int MAX = 10;
        IntStream.range(0, MAX).forEach(i -> dynamap.createTableFromExisting(DummyDocBean.getTableName(), DummyDocBean.getTableName() + "-" + i, true));

        // save and get
        IntStream.range(0, MAX).forEach(i -> {
            String suffix = "-" + i;
            DummyDocBean doc = new DummyDocBean().setId(Integer.toString(i)).setName("test" + i).setWeight(RandomUtils.nextInt(0, 100));
            dynamap.save(doc, true, false, null, suffix);
            doc = new DummyDocBean().setId(Integer.toString(i + 1000)).setName("test" + i).setWeight(RandomUtils.nextInt(0, 100));
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
        ScanResult<DummyDocBean> scanResult = dynamap.scan(scanRequest);
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

    private TestDocumentBean createTestDocumentBean(NestedTypeBean nestedTypeBean) {
        return new TestDocumentBean().setTestId(UUID.randomUUID().toString()).setSequence(1).setNestedObject(nestedTypeBean).setAlias("alias");
    }

    private NestedTypeBean createNestedTypeBean() {
        return new NestedTypeBean().setId(UUID.randomUUID().toString()).setBio("biography");
    }

    private GetObjectRequest<TestDocumentBean> createGetObjectRequest(TestDocumentBean testDocument) {
        return new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testDocument.getTestId()).withRangeKeyValue(testDocument.getRangeKeyValue());
    }

}
