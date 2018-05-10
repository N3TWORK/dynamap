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
import com.google.common.collect.Sets;
import com.n3twork.BatchSaveParams;
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
    public void testToString() {
        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        Assert.assertNotNull(doc.toString());
    }

    @Test
    public void testGetObject() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        // Get Object
        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(testDocumentBean.getId(), doc.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        // Get Not Exists
        Assert.assertNull(dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue("blah").withRangeKeyValue(1), null));

    }

    @Test
    public void testNestedUpdatesSolo() {

        NestedTypeBean nested = createNestedTypeBean();
        nested.setString("string1");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setString("string1");
        dynamap.save(new SaveParams<>(doc));

        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getString(), nested.getString());

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(testDocumentBean);
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(testDocumentBean, nested);
        nestedTypeUpdates.setString("string2");
        dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getNestedObject().getString(), "string2");

    }

    @Test
    public void testStringField() {

        NestedTypeBean nested = createNestedTypeBean();
        nested.setString("string1");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setString("string1");
        dynamap.save(new SaveParams<>(doc));

        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(testDocumentBean.getId(), doc.getId());
        Assert.assertEquals(testDocumentBean.getString(), doc.getString());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getString(), nested.getString());

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(testDocumentBean);
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(testDocumentBean, nested);
        testDocumentUpdates.setString("string2");
        nestedTypeUpdates.setString("string2");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        TestDocument updated = dynamap.update(new UpdateParams<>(testDocumentUpdates));
        Assert.assertEquals(updated.getString(), "string2");
        Assert.assertEquals(updated.getNestedObject().getString(), "string2");

    }

    @Test
    public void testRateLimiters() {

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        NestedTypeBean nested = createNestedTypeBean();
        nested.setString("bio");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setString("String");
        dynamap.save(new SaveParams<>(doc).withWriteLimiter(rateLimiterPair.getWriteLimiter()));
        dynamap.getObject(createGetObjectRequest(doc), rateLimiterPair, null);

        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.setString("new String");
        dynamap.update(new UpdateParams<>(testDocumentUpdates).withWriteLimiter(rateLimiterPair.getWriteLimiter()));

    }

    @Test
    public void testMapOfLong() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));
        Assert.assertNull(nested.getMapOfLongValue("new"));
        Assert.assertEquals(nested.getMapOfLongWithDefaultsValue("new").longValue(), 2L);

        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(doc, nested);
        Assert.assertNull(nestedTypeUpdates.getMapOfLongValue("new"));
        Assert.assertEquals(nestedTypeUpdates.getMapOfLongWithDefaultsValue("new").longValue(), 2L);

        nestedTypeUpdates.incrementMapOfLongAmount("test1", 1L);
        Assert.assertEquals(nestedTypeUpdates.getMapOfLongValue("test1").longValue(), 1L);
        nestedTypeUpdates.incrementMapOfLongAmount("test1", 1L);
        Assert.assertEquals(nestedTypeUpdates.getMapOfLongValue("test1").longValue(), 2L);

        nestedTypeUpdates.incrementMapOfLongWithDefaultsAmount("new", 1L);
        Assert.assertEquals(nestedTypeUpdates.getMapOfLongWithDefaultsValue("new").longValue(), 3L);
        nestedTypeUpdates.incrementMapOfLongWithDefaultsAmount("new", 1L);
        Assert.assertEquals(nestedTypeUpdates.getMapOfLongWithDefaultsValue("new").longValue(), 4L);

        dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getNestedObject().getMapOfLongWithDefaults().get("new").longValue(), 4L);


    }

    @Test
    public void testMapOfCustomObject() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(doc);
        Assert.assertEquals(testDocumentUpdates.getMapOfCustomTypeIds().size(), 0);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates.setMapOfCustomTypeItem(customType1.getName(), customType1);
        Assert.assertTrue(testDocumentUpdates.getMapOfCustomTypeIds().contains("item1"));
        testDocumentUpdates.setMapOfCustomTypeItem(customType2.getName(), customType2);
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(doc, nested);
        nestedTypeUpdates.setMapOfCustomTypeItem(customType1.getName(), customType1);
        nestedTypeUpdates.setMapOfCustomTypeItem(customType2.getName(), customType2);

        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getMapOfCustomTypeItem("item2").getName(), "item2");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeItem("item2").getName(), "item2");

        // Test delete without using current state
        testDocumentUpdates = new TestDocumentUpdates(new TestDocumentBean(), doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.deleteMapOfCustomTypeItem("item1");
        nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), doc.getHashKeyValue(), doc.getRangeKeyValue());
        nestedTypeUpdates.deleteMapOfCustomTypeItem("item1");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        TestDocument testDocument = dynamap.update(new UpdateParams<>(testDocumentUpdates));

        Assert.assertFalse(testDocument.getMapOfCustomTypeIds().contains("item1"));
        Assert.assertFalse(testDocument.getNestedObject().getMapOfCustomTypeIds().contains("item1"));
        Assert.assertTrue(testDocument.getMapOfCustomTypeIds().contains("item2"));
        Assert.assertTrue(testDocument.getNestedObject().getMapOfCustomTypeIds().contains("item2"));
    }

    @Test
    public void testMapOfCustomObjectReplace() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(doc);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType1.getName(), customType1);
        Assert.assertTrue(testDocumentUpdates.getMapOfCustomTypeReplaceIds().contains("item1"));
        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType2.getName(), customType2);
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(doc, nested);
        nestedTypeUpdates.setMapOfCustomTypeReplaceItem(customType1.getName(), customType1);
        nestedTypeUpdates.setMapOfCustomTypeReplaceItem(customType2.getName(), customType2);

        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getMapOfCustomTypeReplaceItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeReplaceItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getMapOfCustomTypeReplaceItem("item2").getName(), "item2");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeReplaceItem("item2").getName(), "item2");

        // Test when without using current state the existing collection is replaced
        CustomType customType3 = new CustomType("item3", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates = new TestDocumentUpdates(new TestDocumentBean(), doc.getHashKeyValue(), doc.getRangeKeyValue());
        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType3.getName(), customType3);
        nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), doc.getHashKeyValue(), doc.getRangeKeyValue());
        nestedTypeUpdates.setMapOfCustomTypeReplaceItem(customType3.getName(), customType3);
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        TestDocument testDocument = dynamap.update(new UpdateParams<>(testDocumentUpdates));

        Assert.assertFalse(testDocument.getMapOfCustomTypeReplaceIds().contains("item1"));
        Assert.assertFalse(testDocument.getNestedObject().getMapOfCustomTypeReplaceIds().contains("item1"));
        Assert.assertFalse(testDocument.getMapOfCustomTypeIds().contains("item2"));
        Assert.assertFalse(testDocument.getNestedObject().getMapOfCustomTypeReplaceIds().contains("item2"));
        Assert.assertTrue(testDocument.getMapOfCustomTypeReplaceIds().contains("item3"));
        Assert.assertTrue(testDocument.getNestedObject().getMapOfCustomTypeReplaceIds().contains("item3"));
    }

    @Test
    public void testMapOfCustomObjectNoDelta() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(doc);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        Map<String, CustomType> map = ImmutableMap.of(customType1.getName(), customType1, customType2.getName(), customType2);

        testDocumentUpdates.setNoDeltaMapOfCustomType(map);
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(doc, nested);
        nestedTypeUpdates.setNoDeltaMapOfCustomType(map);
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        TestDocument updated = dynamap.update(new UpdateParams<>(testDocumentUpdates));

        Assert.assertEquals(updated.getNoDeltaMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(updated.getNestedObject().getNoDeltaMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(updated.getNoDeltaMapOfCustomTypeItem("item2").getName(), "item2");
        Assert.assertEquals(updated.getNestedObject().getNoDeltaMapOfCustomTypeItem("item2").getName(), "item2");
    }

    @Test
    public void testStringSet() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setSetOfString(Sets.newHashSet("test1", "test2"));
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getSetOfString().size(), 2);
        Assert.assertTrue(doc.getSetOfString().containsAll(Sets.newHashSet("test1", "test2")));
        TestDocumentUpdates updates = createTestDocumentUpdates(doc);
        updates.setSetOfStringItem("test3");
        Assert.assertTrue(updates.getSetOfString().contains("test3"));
        TestDocument updated = dynamap.update(new UpdateParams<>(updates));
        Assert.assertTrue(updated.getSetOfString().containsAll(Sets.newHashSet("test1", "test2", "test3")));
    }


    @Test
    public void testQuery() {

        NestedTypeBean nestedTypeBean = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        doc.setIntegerField(1);
        doc.setString("text_to_query");
        dynamap.save(new SaveParams<>(doc));

        QueryRequest<TestDocumentBean> queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(doc.getString())
                .withRangeKeyCondition(new RangeKeyCondition(TestDocumentBean.INTEGERFIELD_FIELD).eq(doc.getIntegerField())).withIndex(TestDocumentBean.GlobalSecondaryIndex.testIndexFull);
        List<TestDocumentBean> testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());
    }

    @Test
    public void testMigration() throws Exception {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        doc.setString("String");
        doc.setNotPersistedString("foo");
        doc.setSetOfString(Sets.newHashSet("test1", "test2"));
        dynamap.save(new SaveParams<>(doc));

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
                item.withString("str", "newString");
            }

            @Override
            public void postMigration(Item item, int version, Object context) {

            }
        });

        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getString(), "newString");
        Assert.assertNull(doc.getNotPersistedString());
    }

    @Test
    public void testDelete() {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNotNull(doc);

        DeleteRequest<TestDocumentBean> deleteRequest = new DeleteRequest<>(TestDocumentBean.class)
                .withHashKeyValue(doc.getId())
                .withRangeKeyValue(doc.getRangeKeyValue());

        dynamap.delete(deleteRequest);
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNull(doc);
    }

    @Test
    public void testOverwrite() {
        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(new SaveParams<>(doc));

        // overwrite allowed
        TestDocumentBean doc2 = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(new SaveParams<>(doc2));

        // overwrite will fail
        try {
            dynamap.save(new SaveParams<>(doc2).withDisableOverwrite(true));
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertNotNull(ex);
        }
    }

    @Test
    public void testIncrementAndSetMapOfLong() {
        String docId1 = UUID.randomUUID().toString();
        Map<String, Long> mapOfLong = new HashMap<>();
        mapOfLong.put("a", 1L);

        TestDocumentBean doc = new TestDocumentBean().setId(docId1).setSequence(1).setMapOfLong(mapOfLong);
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, docId1, 1);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 2);
        Assert.assertEquals(testDocumentUpdates.getMapOfLong().get("a").longValue(), 2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(docId1).withRangeKeyValue(1), null);
        Assert.assertEquals(doc.getMapOfLongValue("a").longValue(), 2);
        Assert.assertEquals(doc.getMapOfLong().get("a").longValue(), 2);

        testDocumentUpdates = new TestDocumentUpdates(doc, docId1, 1);
        testDocumentUpdates.setMapOfLongValue("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertEquals(doc.getMapOfLongValue("a").longValue(), 1);
        Assert.assertEquals(doc.getMapOfLong().get("a").longValue(), 1);

        // Set value then increment - only the set value is considered. Once set value is used, deltas are ignored.
        testDocumentUpdates = new TestDocumentUpdates(doc, docId1, 1);
        testDocumentUpdates.setMapOfLongValue("a", 10L);
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 10L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLong().get("a").longValue(), 10L);

    }

    @Test
    public void testPersistDisabled() {
        NestedTypeBean nestedTypeBean = createNestedTypeBean();
        nestedTypeBean.setNotPersistedString("foo");
        TestDocumentBean doc = createTestDocumentBean(nestedTypeBean);
        doc.setNotPersistedString("foo");
        dynamap.save(new SaveParams<>(doc));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNull(doc.getNotPersistedString());
        Assert.assertNull(doc.getNestedObject().getNotPersistedString());

        TestDocumentUpdates testDocumentUpdates = new TestDocumentUpdates(doc, doc.getId(), doc.getRangeKeyValue());
        testDocumentUpdates.setString("newString");
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectRequest(doc), null);
        Assert.assertNull(doc.getNotPersistedString());
        Assert.assertNull(doc.getNestedObject().getNotPersistedString());
    }


    @Test
    public void testBatchGetItem() {
        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();
        String docId2 = UUID.randomUUID().toString();
        String nestedId2 = UUID.randomUUID().toString();
        dynamap.save(new SaveParams<>((new TestDocumentBean().setId(docId1).setSequence(1).setString("String")
                .setNestedObject(new NestedTypeBean().setId(nestedId1)))));
        dynamap.save(new SaveParams<>(new TestDocumentBean().setId(docId2).setSequence(1).setString("String")
                .setNestedObject(new NestedTypeBean().setId(nestedId2))));


        List<TestDocumentBean> testDocuments;

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        BatchGetObjectRequest<TestDocumentBean> batchGetObjectRequest = new BatchGetObjectRequest<TestDocumentBean>()
                .withGetObjectRequests(ImmutableList.of(
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(docId1).withRangeKeyValue(1),
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(docId2).withRangeKeyValue(1)))
                .withRateLimiters(rateLimiterPair);

        testDocuments = dynamap.batchGetObjectSingleCollection(batchGetObjectRequest);

        Assert.assertEquals(testDocuments.size(), 2);
    }

    @Test
    public void testOptimisticLocking() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean().setId(DOC_ID).setName("test").setWeight(6);
        dynamap.save(new SaveParams<>(doc));

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        DummyDocUpdates docUpdates = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());
        docUpdates.setWeight(100);
        dynamap.update(new UpdateParams<>(docUpdates));

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);


        // two simultaneous updates, second one should fail
        DummyDocUpdates docUpdates1 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());
        DummyDocUpdates docUpdates2 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue());

        dynamap.update(new UpdateParams<>(docUpdates1));

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

        try {
            dynamap.update(new UpdateParams<>(docUpdates2));
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
        }

        // optimist locking disabled
        DummyDocUpdates docUpdates3 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        DummyDocUpdates docUpdates4 = new DummyDocUpdates(savedDoc, savedDoc.getHashKeyValue(), true);
        dynamap.update(new UpdateParams<>(docUpdates3));
        dynamap.update(new UpdateParams<>(docUpdates4));

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

    }

    @Test
    public void testConditionalChecks() {

        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setMapOfLong(ImmutableMap.of("dollars", 1L, "francs", 1L));

        TestDocumentBean doc = new TestDocumentBean().setId(docId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setString("String");
        dynamap.save(new SaveParams<>(doc));

        // add 1 to dollars and a check to ensure it is less than 2
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), docId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        // increment again and check that conditional exception is thrown
        nestedTypeUpdates = new NestedTypeUpdates(new NestedTypeBean(), docId1, 1);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        boolean errorThrown = false;
        try {
            dynamap.update(new UpdateParams<>(nestedTypeUpdates));
        } catch (ConditionalCheckFailedException e) {
            errorThrown = true;
        }
        Assert.assertTrue(errorThrown);

    }

    @Test
    public void testOptimisticLockingWithSave() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean().setId(DOC_ID).setName("test").setWeight(6);
        dynamap.save(new SaveParams<>(doc));

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        savedDoc.setWeight(100);

        // two simultaneous updates, second one should fail
        dynamap.save(new SaveParams<>(savedDoc));

        try {
            dynamap.save(new SaveParams<>(savedDoc));
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
        }

        savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID), null);

        // two simultaneous updates, second one should fail
        dynamap.save(new SaveParams<>(savedDoc).withDisableOptimisticLocking(true));
        // two simultaneous updates, second one should fail
        dynamap.save(new SaveParams<>(savedDoc).withDisableOptimisticLocking(true));

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);
    }

    @Test
    public void testBatchSaveAndScan() {
        batchSave(null);
    }

    @Test
    public void testBatchSaveAndScanWithRateLimiter() {
        Map<Class, DynamoRateLimiter> limiterMap = new HashMap<>();
        limiterMap.put(TestDocumentBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));
        limiterMap.put(DummyDocBean.class, new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 100));

        //TODO: need to do a better test the verifies correct limiter behavior
        batchSave(limiterMap);
    }

    private void batchSave(Map<Class, DynamoRateLimiter> limiterMap) {
        final int TEST_DOCS_SIZE = 22;
        final int DUMMY_DOCS_SIZE = 23;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> testDocsIds = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String bigString = new String(new char[10000]).replace('\0', 'X');

        for (int i = 0; i < TEST_DOCS_SIZE; i++) {
            TestDocumentBean testDocument = createTestDocumentBean(createNestedTypeBean());
            testDocsIds.add(testDocument.getId());
            docsToSave.add(testDocument);
        }

        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean().setId(id).setName(bigString).setWeight(i);
            dummyDocsIds.add(id);
            docsToSave.add(doc);
        }

        dynamap.batchSave(new BatchSaveParams<>(docsToSave).withWriteLimiters(limiterMap));

        ScanRequest<TestDocumentBean> scanRequest = new ScanRequest<>(TestDocumentBean.class);
        ScanResult<TestDocumentBean> scanResult = dynamap.scan(scanRequest);
        List<TestDocumentBean> savedTestDocs = scanResult.getResults();

        ScanRequest<DummyDocBean> scanRequest2 = new ScanRequest<>(DummyDocBean.class);
        ScanResult<DummyDocBean> scanResult2 = dynamap.scan(scanRequest2);
        List<DummyDocBean> savedDummyDocs = scanResult2.getResults();

        Assert.assertEquals(savedTestDocs.size(), TEST_DOCS_SIZE);
        Assert.assertEquals(savedDummyDocs.size(), DUMMY_DOCS_SIZE);

        List<String> savedTestDocsIds = savedTestDocs.stream().map(TestDocumentBean::getId).collect(Collectors.toList());
        List<String> savedDummyDocsIds = savedDummyDocs.stream().map(DummyDocBean::getId).collect(Collectors.toList());

        Assert.assertTrue(savedTestDocsIds.containsAll(testDocsIds) && testDocsIds.containsAll(savedTestDocsIds));
        Assert.assertTrue(savedDummyDocsIds.containsAll(dummyDocsIds) && dummyDocsIds.containsAll(savedDummyDocsIds));

        // Test start exclusive
        ScanRequest<TestDocumentBean> scanRequest3 = new ScanRequest<>(TestDocumentBean.class).withStartExclusiveHashKeyValue(testDocsIds.get(3));
        ScanResult<TestDocumentBean> scanResult3 = dynamap.scan(scanRequest3);
        Assert.assertTrue(scanResult3.getResults().size() > 0);


    }

    @Test
    public void testBeanSubclass() {
        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setString("biography");

        TestDocumentBean doc = new TestDocumentBean().setId(docId1).setSequence(1)
                .setSomeList(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setString("String");
        dynamap.save(new SaveParams<>(doc));

        GetObjectRequest<TestDocumentBeanSubclass> getObjectRequest = new GetObjectRequest<>(TestDocumentBeanSubclass.class).withHashKeyValue(docId1).withRangeKeyValue(1);
        TestDocumentBeanSubclass testDocumentBean = dynamap.getObject(getObjectRequest, null);

        Assert.assertNotNull(testDocumentBean.getId(), docId1);
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
            dynamap.save(new SaveParams<>(doc).withSuffix(suffix));
            doc = new DummyDocBean().setId(Integer.toString(i + 1000)).setName("test" + i).setWeight(RandomUtils.nextInt(0, 100));
            dynamap.save(new SaveParams<>(doc).withSuffix(suffix));
            DummyDocBean savedDoc = dynamap.getObject(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i)).withSuffix(suffix), null);
            Assert.assertEquals(savedDoc.getId(), Integer.toString(i));
        });

        // batch get
        List<GetObjectRequest<DummyDocBean>> getObjectRequests = IntStream.range(0, MAX).mapToObj(i -> {
            String suffix = "-" + i;
            return new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i + 1000)).withSuffix(suffix);
        }).collect(Collectors.toList());

        BatchGetObjectRequest<DummyDocBean> batchRequest = new BatchGetObjectRequest<DummyDocBean>().withGetObjectRequests(getObjectRequests);
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
        dynamap.update(new UpdateParams<>(docUpdates).withSuffix(suffix));

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
        dynamap.batchSave(new BatchSaveParams<>(docsToSave).withSuffix(suffix2));

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

        dynamap.batchSave(new BatchSaveParams<>(docsToSave));

        QueryRequest<DummyDoc2Bean> queryRequest = new QueryRequest<>(DummyDoc2Bean.class)
                .withHashKeyValue(id)
                .withRangeKeyCondition(new RangeKeyCondition(DummyDocBean.WEIGHT_FIELD).eq(0))
                .withIndex(DummyDoc2Bean.LocalSecondaryIndex.weightIndex);

        List<DummyDoc2Bean> docs = dynamap.query(queryRequest);
        Assert.assertEquals(docs.size(), 1);

    }

    @Test
    public void testGlobalSecondaryIndex() {
        for (int i = 0; i < 10; i++) {
            TestDocumentBean testDocumentBean = createTestDocumentBean(null);
            testDocumentBean.setString("test");
            testDocumentBean.setIntegerField(i);
            testDocumentBean.setSetOfString(Sets.newHashSet("ignore1", "ignore2"));
            dynamap.save(new SaveParams<>(testDocumentBean));
        }

        QueryRequest<TestDocumentBean> queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withHashKeyValue("test")
                .withRangeKeyCondition(new RangeKeyCondition(TestDocumentBean.INTEGERFIELD_FIELD).lt(5))
                .withIndex(TestDocumentBean.GlobalSecondaryIndex.testIndexProjection);

        List<TestDocumentBean> docs = dynamap.query(queryRequest);
        Assert.assertEquals(docs.size(), 5);
        Assert.assertEquals(docs.get(0).getSetOfString().size(), 0); // ensure that non projected fields are not populated
    }


    @Test
    public void testReturnUpdated() {

        NestedTypeBean nested = createNestedTypeBean();
        nested.setString("string1");
        nested.setIntegerField(1);
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setString("string1");
        doc.setIntegerField(1);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = createTestDocumentUpdates(doc);
        doc.setString("string2");
        NestedTypeUpdates nestedTypeUpdates = createNestedTypeUpdates(doc, nested);
        testDocumentUpdates.setString("string2");
        nestedTypeUpdates.setString("string2");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);

        TestDocument updated = dynamap.update(new UpdateParams<>(testDocumentUpdates).withReturnValue(DynamapReturnValue.UPDATED_NEW));
        Assert.assertEquals(updated.getIntegerField().intValue(), 1);
        Assert.assertEquals(updated.getNestedObject().getIntegerField().intValue(), 1);
        Assert.assertEquals(updated.getString(), "string2");
        Assert.assertEquals(updated.getNestedObject().getString(), "string2");

        testDocumentUpdates = createTestDocumentUpdates(updated);
        testDocumentUpdates.setString("string3");
        updated = dynamap.update(new UpdateParams<>(testDocumentUpdates).withReturnValue(DynamapReturnValue.UPDATED_OLD));
        Assert.assertEquals(updated.getString(), "string2");


    }

    private TestDocumentBean createTestDocumentBean(NestedTypeBean nestedTypeBean) {
        return new TestDocumentBean().setId(UUID.randomUUID().toString()).setSequence(1).setNestedObject(nestedTypeBean);
    }

    private NestedTypeBean createNestedTypeBean() {
        return new NestedTypeBean().setId(UUID.randomUUID().toString());
    }

    private GetObjectRequest<TestDocumentBean> createGetObjectRequest(TestDocumentBean testDocument) {
        return new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testDocument.getId()).withRangeKeyValue(testDocument.getRangeKeyValue());
    }

    private TestDocumentUpdates createTestDocumentUpdates(TestDocument testDocument) {
        return new TestDocumentUpdates(testDocument, testDocument.getId(), testDocument.getSequence());
    }

    private NestedTypeUpdates createNestedTypeUpdates(TestDocument testDocument, NestedType nestedType) {
        return new NestedTypeUpdates(nestedType, testDocument.getId(), testDocument.getSequence());
    }

}
