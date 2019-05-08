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
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.stream.Stream;

public class DynamapTest {

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
        schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"),
                getClass().getResourceAsStream("/DummySchema.json"),
                getClass().getResourceAsStream("/DummyLocalIndexSchema.json"));
        // Create tables
        dynamap = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        dynamap.createTables(System.getProperty("aws.profile") == null);
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
        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(testDocumentBean.getId(), doc.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        // Get Not Exists
        Assert.assertNull(dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue("blah").withRangeKeyValue(1))));

    }

    @Test
    public void testStringField() {
        NestedTypeBean nested = createNestedTypeBean();
        nested.setString("string1");
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setString("string1");
        dynamap.save(new SaveParams<>(doc));

        TestDocumentBean testDocumentBean = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(testDocumentBean.getId(), doc.getId());
        Assert.assertEquals(testDocumentBean.getString(), doc.getString());
        Assert.assertEquals(testDocumentBean.getNestedObject().getId(), nested.getId());
        Assert.assertEquals(testDocumentBean.getNestedObject().getString(), nested.getString());

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = testDocumentBean.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
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
        dynamap.getObject(createGetObjectParams(doc));

        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
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

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
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

        TestDocumentUpdates documentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getNestedObject().getMapOfLongWithDefaults().get("new").longValue(), 4L);


    }

    @Test
    public void testMapOfCustomObject() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        Assert.assertEquals(testDocumentUpdates.getMapOfCustomTypeIds().size(), 0);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates.setMapOfCustomTypeItem(customType1.getName(), customType1);
        Assert.assertTrue(testDocumentUpdates.getMapOfCustomTypeIds().contains("item1"));
        testDocumentUpdates.setMapOfCustomTypeItem(customType2.getName(), customType2);
        nestedTypeUpdates.setMapOfCustomTypeItem(customType1.getName(), customType1);
        nestedTypeUpdates.setMapOfCustomTypeItem(customType2.getName(), customType2);

        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getMapOfCustomTypeItem("item2").getName(), "item2");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeItem("item2").getName(), "item2");

        // Test delete without using current state
        testDocumentUpdates = new TestDocumentBean(doc.getId(), doc.getSequence()).createUpdates();
        testDocumentUpdates.deleteMapOfCustomTypeItem("item1");
        nestedTypeUpdates = new NestedTypeBean().createUpdates();
        nestedTypeUpdates.deleteMapOfCustomTypeItem("item1");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        TestDocument testDocument = dynamap.update(new UpdateParams<>(testDocumentUpdates));

        Assert.assertFalse(testDocument.getMapOfCustomTypeIds().contains("item1"));
        Assert.assertFalse(testDocument.getNestedObject().getMapOfCustomTypeIds().contains("item1"));
        Assert.assertTrue(testDocument.getMapOfCustomTypeIds().contains("item2"));
        Assert.assertTrue(testDocument.getNestedObject().getMapOfCustomTypeIds().contains("item2"));

        // Test overwrite entire nested object
        //TODO: Currently overwriting nested generated objects that use compression or serialize as list is not supported
        // Hence this test is using another generated type
        NestedType2 nestedType2 = new NestedType2Bean().setId(UUID.randomUUID().toString());
        testDocumentUpdates = doc.createUpdates().setNestedObject2(nestedType2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        testDocumentUpdates = doc.createUpdates().setNestedObject2(new NestedType2Bean().setId(UUID.randomUUID().toString()));
        testDocument = dynamap.update(new UpdateParams<>(testDocumentUpdates));
        Assert.assertFalse(testDocument.getNestedObject2().getId().equals(nestedType2.getId()));

    }

    @Test
    public void testPersistMapAsList() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        Assert.assertEquals(testDocumentUpdates.getListMapOfCustomType().size(), 0);

        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        testDocumentUpdates.setListMapOfCustomTypeItem(customType1.getName(), customType1);
        testDocumentUpdates.setListMapOfCustomTypeItem(customType2.getName(), customType2);
        nestedTypeUpdates.setListMapOfCustomTypeItem(customType1.getName(), customType1);
        nestedTypeUpdates.setListMapOfCustomTypeItem(customType2.getName(), customType2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getListMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getListMapOfCustomTypeItem("item2").getName(), "item2");
        Assert.assertEquals(doc.getNestedObject().getListMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getNestedObject().getListMapOfCustomTypeItem("item2").getName(), "item2");
    }

    @Test
    public void testPersistGzipMapAsList() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        Assert.assertEquals(testDocumentUpdates.getGzipListMapOfCustomType().size(), 0);

        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        testDocumentUpdates.setGzipListMapOfCustomTypeItem(customType1.getName(), customType1);
        testDocumentUpdates.setGzipListMapOfCustomTypeItem(customType2.getName(), customType2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getGzipListMapOfCustomTypeItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getGzipListMapOfCustomTypeItem("item2").getName(), "item2");
    }

    @Test
    public void testMapOfCustomObjectReplace() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType1.getName(), customType1);
        Assert.assertTrue(testDocumentUpdates.getMapOfCustomTypeReplaceIds().contains("item1"));
        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType2.getName(), customType2);
        nestedTypeUpdates.setMapOfCustomTypeReplaceItem(customType1.getName(), customType1);
        nestedTypeUpdates.setMapOfCustomTypeReplaceItem(customType2.getName(), customType2);

        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getMapOfCustomTypeReplaceItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeReplaceItem("item1").getName(), "item1");
        Assert.assertEquals(doc.getMapOfCustomTypeReplaceItem("item2").getName(), "item2");
        Assert.assertEquals(doc.getNestedObject().getMapOfCustomTypeReplaceItem("item2").getName(), "item2");

        // Test when without using current state the existing collection is replaced
        CustomType customType3 = new CustomType("item3", "test", CustomType.CustomTypeEnum.VALUE_A);

        testDocumentUpdates = new TestDocumentBean(doc.getId(), doc.getSequence()).createUpdates();
        testDocumentUpdates.setMapOfCustomTypeReplaceItem(customType3.getName(), customType3);
        nestedTypeUpdates = new NestedTypeBean().createUpdates();
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

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        Map<String, CustomType> map = ImmutableMap.of(customType1.getName(), customType1, customType2.getName(), customType2);

        testDocumentUpdates.setNoDeltaMapOfCustomType(map);
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
        dynamap.save(new SaveParams<>(doc));
        // test adding from empty
        TestDocumentUpdates updates = doc.createUpdates();
        updates.setSetOfStringItem("test1");
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getSetOfString().size(), 1);

        // delete the item so that it becomes an empty set
        updates = doc.createUpdates();
        updates.deleteSetOfStringItem("test1");
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertTrue(doc.getSetOfString().size() == 0);

        doc.setSetOfString(Sets.newHashSet("test1", "test2"));
        dynamap.save(new SaveParams<>(doc));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getSetOfString().size(), 2);
        Assert.assertTrue(doc.getSetOfString().containsAll(Sets.newHashSet("test1", "test2")));
        updates = doc.createUpdates();
        updates.setSetOfStringItem("test3");
        Assert.assertTrue(updates.getSetOfString().contains("test3"));
        TestDocument updated = dynamap.update(new UpdateParams<>(updates));
        Assert.assertTrue(updated.getSetOfString().containsAll(Sets.newHashSet("test1", "test2", "test3")));
    }

    @Test
    public void testStringSetNoDeltas() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));
        TestDocumentUpdates updates = doc.createUpdates();
        updates.setSetOfStringNoDeltas(Sets.newHashSet("test1"));
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getSetOfStringNoDeltas().size(), 1);
        updates = doc.createUpdates();
        updates.setSetOfStringNoDeltas(Collections.emptySet());
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getSetOfStringNoDeltas().size(), 0);
    }

    @Test
    public void testNumberSet() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        doc.setSetOfLong(Sets.newHashSet(1L, 2L));
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getSetOfLong().size(), 2);
        Assert.assertTrue(doc.getSetOfLong().containsAll(Sets.newHashSet(1L, 2L)));
        TestDocumentUpdates updates = doc.createUpdates();
        updates.setSetOfLongValue(3L);
        Assert.assertTrue(updates.getSetOfLong().contains(3L));
        TestDocument updated = dynamap.update(new UpdateParams<>(updates));
        Assert.assertTrue(updated.getSetOfLong().containsAll(Sets.newHashSet(1L, 2L, 3L)));
    }

    @Test
    public void testGzipSet() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        Assert.assertEquals(testDocumentUpdates.getGzipSetOfCustomType().size(), 0);

        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        testDocumentUpdates.setGzipSetOfCustomTypeItem(customType1);
        testDocumentUpdates.setGzipSetOfCustomTypeItem(customType2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertTrue(doc.getGzipSetOfCustomType().contains(customType1));
        Assert.assertTrue(doc.getGzipSetOfCustomType().contains(customType2));
    }

    @Test
    public void testListOfInteger() {
        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getListOfInteger().size(), 0);
        TestDocumentUpdates updates = doc.createUpdates();
        updates.addListOfIntegerValue(1);
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getListOfInteger().size(), 1);
        updates = doc.createUpdates();
        updates.addListOfIntegerValue(1);
        updates.addListOfIntegerValue(2);
        Assert.assertEquals(updates.getListOfInteger().size(), 3);
        dynamap.update(new UpdateParams<>(updates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getListOfInteger().size(), 3);
    }

    @Test
    public void testGzipList() {

        NestedTypeBean nested = createNestedTypeBean();
        TestDocumentBean doc = createTestDocumentBean(nested);
        dynamap.save(new SaveParams<>(doc));

        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        Assert.assertEquals(testDocumentUpdates.getGzipListOfCustomType().size(), 0);

        CustomType customType1 = new CustomType("item1", "test", CustomType.CustomTypeEnum.VALUE_A);
        CustomType customType2 = new CustomType("item2", "test", CustomType.CustomTypeEnum.VALUE_A);
        testDocumentUpdates.setGzipListOfCustomType(Arrays.asList(customType1, customType2));
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertTrue(doc.getGzipListOfCustomType().contains(customType1));
        Assert.assertTrue(doc.getGzipListOfCustomType().contains(customType2));
    }


    @Test
    public void testQuery() {
        QueryRequest<TestDocumentBean> queryRequest;
        List<TestDocumentBean> testDocuments;

        NestedTypeBean nestedTypeBean = createNestedTypeBean();
        String hashKey = UUID.randomUUID().toString();
        TestDocumentBean doc1 = createTestDocumentBean(hashKey, createNestedTypeBean());
        doc1.setIntegerField(1);
        doc1.setString("text_to_query");
        dynamap.save(new SaveParams<>(doc1));
        TestDocumentBean doc2 = createTestDocumentBean(hashKey, createNestedTypeBean());
        doc2.setIntegerField(2);
        doc2.setString("other_text_to_query");
        dynamap.save(new SaveParams<>(doc2));

        //Legacy API
        queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(hashKey)
                .withRangeKeyCondition(new RangeKeyCondition(TestDocumentBean.SEQUENCE_FIELD).eq(doc1.getSequence()));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(hashKey)
                .withRangeKeyCondition(new RangeKeyCondition(TestDocumentBean.SEQUENCE_FIELD).ge(doc1.getSequence()));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 2);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());
        Assert.assertEquals(testDocuments.get(1).getSequence(), doc2.getSequence());

        queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(hashKey)
                .addQueryFilter(new QueryFilter(TestDocument.STRING_FIELD).beginsWith("text"));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(hashKey)
                .addQueryFilter(new QueryFilter(TestDocument.INTEGERFIELD_FIELD).eq(1));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class).withHashKeyValue(hashKey)
                .addQueryFilter(new QueryFilter(TestDocument.INTEGERFIELD_FIELD).eq(3));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 0);


        //Expression API
        queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withKeyConditionExpression(String.format("%s = :hashKey and %s = :rangeKey", TestDocument.ID_FIELD, TestDocument.SEQUENCE_FIELD))
                .withValues(new ValueMap().withString(":hashKey", doc1.getId()).withInt(":rangeKey", doc1.getSequence()));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withKeyConditionExpression(String.format("%s = :hashKey", TestDocument.ID_FIELD))
                .withFilterExpression(String.format("begins_with(%s, :str)", TestDocument.STRING_FIELD))
                .withValues(new ValueMap().withString(":hashKey", doc1.getId()).withString(":str", "text"));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withKeyConditionExpression(String.format("%s = :hashKey", TestDocument.ID_FIELD))
                .withFilterExpression(String.format("%s = :int", TestDocument.INTEGERFIELD_FIELD))
                .withValues(new ValueMap().withString(":hashKey", doc1.getId()).withInt(":int", 1));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 1);
        Assert.assertEquals(testDocuments.get(0).getSequence(), doc1.getSequence());
        Assert.assertEquals(testDocuments.get(0).getNestedObject().getString(), nestedTypeBean.getString());

        queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withKeyConditionExpression(String.format("%s = :hashKey", TestDocument.ID_FIELD))
                .withFilterExpression(String.format("%s = :int", TestDocument.INTEGERFIELD_FIELD))
                .withValues(new ValueMap().withString(":hashKey", doc1.getId()).withInt(":int", 3));
        testDocuments = dynamap.query(queryRequest);
        Assert.assertEquals(testDocuments.size(), 0);
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

        Dynamap dynamap2 = new Dynamap(ddb, schemaRegistry).withPrefix("test").withObjectMapper(objectMapper);
        doc = dynamap2.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getString(), "newString");
        Assert.assertNull(doc.getNotPersistedString());

        // Test that old code cannot load migrated data
        boolean exceptionThrown = false;
        try {
            doc = dynamap.getObject(createGetObjectParams(doc));
        } catch (Exception e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().contains("Document schema has been migrated to a version later than this release supports"));
        }
        Assert.assertTrue(exceptionThrown);

        // Test that old code cannot update migrated data
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        testDocumentUpdates.setString("foobar");
        exceptionThrown = false;
        try {
            dynamap.update(new UpdateParams<>(testDocumentUpdates));
        } catch (Exception e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().contains("The conditional request failed"));
        }
        Assert.assertTrue(exceptionThrown);

    }

    @Test
    public void testDelete() {

        TestDocumentBean doc = createTestDocumentBean(createNestedTypeBean());
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertNotNull(doc);

        DeleteRequest<TestDocumentBean> deleteRequest = new DeleteRequest<>(TestDocumentBean.class)
                .withHashKeyValue(doc.getId())
                .withRangeKeyValue(doc.getRangeKeyValue());

        dynamap.delete(deleteRequest);
        doc = dynamap.getObject(createGetObjectParams(doc));
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
    public void testIncrementDecrementInteger() {

        TestDocumentBean doc = createTestDocumentBean(null);
        dynamap.save(new SaveParams<>(doc));
        Assert.assertNull(doc.getIntegerField());

        TestDocumentUpdates documentUpdates = doc.createUpdates();
        documentUpdates.incrementIntegerField(1);
        Assert.assertEquals(documentUpdates.getIntegerField().intValue(), 1);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getIntegerField().intValue(), 1);

        doc = createTestDocumentBean(null);
        dynamap.save(new SaveParams<>(doc));
        documentUpdates = doc.createUpdates();
        documentUpdates.decrementIntegerField(1);
        Assert.assertEquals(documentUpdates.getIntegerField().intValue(), -1);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getIntegerField().intValue(), -1);

        documentUpdates = doc.createUpdates();
        documentUpdates.setIntegerField(null);
        Assert.assertNull(documentUpdates.getIntegerField());
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertNull(doc.getIntegerField());

    }

    @Test
    public void testIncrementDecrementIntegerWithNonZeroDefault() {

        TestDocumentBean doc = createTestDocumentBean(null);
        dynamap.save(new SaveParams<>(doc));
        Assert.assertEquals(doc.getIntegerFieldNonZeroDefault().intValue(), 2);

        TestDocumentUpdates documentUpdates = doc.createUpdates();
        documentUpdates.incrementIntegerFieldNonZeroDefault(1);
        Assert.assertEquals(documentUpdates.getIntegerFieldNonZeroDefault().intValue(), 3);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getIntegerFieldNonZeroDefault().intValue(), 3);

        doc = createTestDocumentBean(null);
        dynamap.save(new SaveParams<>(doc));
        documentUpdates = doc.createUpdates();
        documentUpdates.decrementIntegerFieldNonZeroDefault(1);
        Assert.assertEquals(documentUpdates.getIntegerFieldNonZeroDefault().intValue(), 1);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getIntegerFieldNonZeroDefault().intValue(), 1);

        documentUpdates = doc.createUpdates();
        documentUpdates.setIntegerFieldNonZeroDefault(null);
        Assert.assertEquals(documentUpdates.getIntegerFieldNonZeroDefault().intValue(), 2);
        dynamap.update(new UpdateParams<>(documentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getIntegerFieldNonZeroDefault().intValue(), 2);

    }

    @Test
    public void testIncrementAndSetMapOfLong() {
        String docId1 = UUID.randomUUID().toString();
        Map<String, Long> mapOfLong = new HashMap<>();
        mapOfLong.put("a", 1L);

        TestDocumentBean doc = new TestDocumentBean(docId1, 1).setMapOfLong(mapOfLong);
        dynamap.save(new SaveParams<>(doc));

        doc = dynamap.getObject(createGetObjectParams(doc));
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 2);
        Assert.assertEquals(testDocumentUpdates.getMapOfLong().get("a").longValue(), 2);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getMapOfLongValue("a").longValue(), 2);
        Assert.assertEquals(doc.getMapOfLong().get("a").longValue(), 2);

        testDocumentUpdates = doc.createUpdates();
        testDocumentUpdates.setMapOfLongValue("a", 1L);
        Assert.assertEquals(testDocumentUpdates.getMapOfLongValue("a").longValue(), 1);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertEquals(doc.getMapOfLongValue("a").longValue(), 1);
        Assert.assertEquals(doc.getMapOfLong().get("a").longValue(), 1);

        // Set value then increment - only the set value is considered. Once set value is used, deltas are ignored.
        testDocumentUpdates = doc.createUpdates();
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
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertNull(doc.getNotPersistedString());
        Assert.assertNull(doc.getNestedObject().getNotPersistedString());

        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        testDocumentUpdates.setString("newString");
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        doc = dynamap.getObject(createGetObjectParams(doc));
        Assert.assertNull(doc.getNotPersistedString());
        Assert.assertNull(doc.getNestedObject().getNotPersistedString());
    }


    @Test
    public void testBatchGetItem() {
        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();
        String docId2 = UUID.randomUUID().toString();
        String nestedId2 = UUID.randomUUID().toString();
        dynamap.save(new SaveParams<>((new TestDocumentBean(docId1, 1).setString("String")
                .setNestedObject(new NestedTypeBean().setId(nestedId1)))));
        dynamap.save(new SaveParams<>(new TestDocumentBean(docId2, 1).setString("String")
                .setNestedObject(new NestedTypeBean().setId(nestedId2))));


        List<TestDocumentBean> testDocuments;

        ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
                new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 20));

        BatchGetObjectParams<TestDocumentBean> batchGetObjectParams = new BatchGetObjectParams<TestDocumentBean>()
                .withGetObjectRequests(ImmutableList.of(
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(docId1).withRangeKeyValue(1),
                        new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(docId2).withRangeKeyValue(1)))
                .withRateLimiters(rateLimiterPair);

        testDocuments = dynamap.batchGetObjectSingleCollection(batchGetObjectParams);

        Assert.assertEquals(testDocuments.size(), 2);
    }

    @Test
    public void testOptimisticLocking() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean(DOC_ID).setName("test").setWeight(6L);
        dynamap.save(new SaveParams<>(doc));

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        DummyDocUpdates docUpdates = savedDoc.createUpdates();
        docUpdates.setWeight(100L);
        dynamap.update(new UpdateParams<>(docUpdates));

        savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));

        Assert.assertEquals(savedDoc.getRevision().intValue(), 2);


        // two simultaneous updates, second one should fail
        DummyDocUpdates docUpdates1 = savedDoc.createUpdates();
        DummyDocUpdates docUpdates2 = savedDoc.createUpdates();

        dynamap.update(new UpdateParams<>(docUpdates1));

        savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

        try {
            dynamap.update(new UpdateParams<>(docUpdates2));
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
        }

        // optimistic locking disabled
        DummyDocUpdates docUpdates3 = savedDoc.createUpdates().setDisableOptimisticLocking(true);
        DummyDocUpdates docUpdates4 = savedDoc.createUpdates().setDisableOptimisticLocking(true);
        dynamap.update(new UpdateParams<>(docUpdates3));
        dynamap.update(new UpdateParams<>(docUpdates4));

        savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));
        Assert.assertEquals(savedDoc.getRevision().intValue(), 3);

    }

    @Test
    public void testConditionalChecks() {

        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setMapOfLong(ImmutableMap.of("dollars", 1L, "francs", 1L));

        TestDocumentBean doc = new TestDocumentBean(docId1, 1)
                .setListOfString(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setString("String");
        dynamap.save(new SaveParams<>(doc));

        // add 1 to dollars and a check to ensure it is less than 2
        NestedTypeUpdates nestedTypeUpdates = new NestedTypeBean().createUpdates();
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates();
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        dynamap.update(new UpdateParams<>(testDocumentUpdates));
        // increment again and check that conditional exception is thrown
        nestedTypeUpdates = new NestedTypeBean().createUpdates();
        testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        nestedTypeUpdates.incrementMapOfLongAmount("dollars", 1L);
        nestedTypeUpdates.getExpressionBuilder().addCheckMapValuesCondition(TestDocumentBean.NESTEDOBJECT_FIELD, NestedTypeBean.MAPOFLONG_FIELD,
                ImmutableMap.of("dollars", 2L), DynamoExpressionBuilder.ComparisonOperator.LESS_THAN);
        boolean errorThrown = false;
        try {
            dynamap.update(new UpdateParams<>(testDocumentUpdates));
        } catch (ConditionalCheckFailedException e) {
            errorThrown = true;
        }
        Assert.assertTrue(errorThrown);

    }

    @Test
    public void testOptimisticLockingWithSave() {
        final String DOC_ID = "1";
        DummyDocBean doc = new DummyDocBean(DOC_ID).setName("test").setWeight(6L);
        dynamap.save(new SaveParams<>(doc));

        DummyDocBean savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));

        Assert.assertEquals(savedDoc.getRevision().intValue(), 1);

        savedDoc.setWeight(100L);

        // two simultaneous updates, second one should fail
        dynamap.save(new SaveParams<>(savedDoc));

        try {
            dynamap.save(new SaveParams<>(savedDoc));
            Assert.fail();
        } catch (ConditionalCheckFailedException ex) {
            Assert.assertNotNull(ex);
        }

        savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(DOC_ID)));

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

        for (long i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean(id).setName(bigString).setWeight(i);
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


    }

    @Test
    public void testParallelScan() {
        final int TEST_DOCS_SIZE = 22;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> testDocsIds = new ArrayList<>();

        for (int i = 0; i < TEST_DOCS_SIZE; i++) {
            TestDocumentBean testDocument = createTestDocumentBean(createNestedTypeBean());
            testDocsIds.add(testDocument.getId());
            docsToSave.add(testDocument);
        }

        dynamap.batchSave(new BatchSaveParams<>(docsToSave));

        ScanRequest<TestDocumentBean> scanRequest = new ScanRequest<>(TestDocumentBean.class)
                .withTotalSegments(2)
                .withSegment(0);

        ScanResult<TestDocumentBean> scanResult = dynamap.scan(scanRequest);
        List<TestDocumentBean> savedTestDocs = scanResult.getResults();

        ScanRequest<TestDocumentBean> scanRequest2 = new ScanRequest<>(TestDocumentBean.class)
                .withTotalSegments(2)
                .withSegment(1);

        ScanResult<TestDocumentBean> scanResult2 = dynamap.scan(scanRequest2);
        List<TestDocumentBean> savedTestDocs2 = scanResult2.getResults();

        Assert.assertEquals(savedTestDocs.size() + savedTestDocs2.size(), TEST_DOCS_SIZE);

        List<String> savedTestDocsIds = Stream.concat(savedTestDocs.stream(), savedTestDocs2.stream())
                .map(TestDocumentBean::getId).collect(Collectors.toList());

        Assert.assertTrue(savedTestDocsIds.containsAll(testDocsIds) && testDocsIds.containsAll(savedTestDocsIds));
    }

    @Test
    public void testBeanSubclass() {
        String docId1 = UUID.randomUUID().toString();
        String nestedId1 = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean().setId(nestedId1).setString("biography");

        TestDocumentBean doc = new TestDocumentBean(docId1, 1)
                .setListOfString(Arrays.asList("test1", "test2")).setNestedObject(nestedObject).setString("String");
        dynamap.save(new SaveParams<>(doc));

        GetObjectRequest<TestDocumentBeanSubclass> getObjectRequest = new GetObjectRequest<>(TestDocumentBeanSubclass.class).withHashKeyValue(docId1).withRangeKeyValue(1);
        TestDocumentBeanSubclass testDocumentBean = dynamap.getObject(new GetObjectParams<>(getObjectRequest));

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
            DummyDocBean doc = new DummyDocBean(Integer.toString(i)).setName("test" + i).setWeight(RandomUtils.nextLong(0, 100));
            dynamap.save(new SaveParams<>(doc).withSuffix(suffix));
            doc = new DummyDocBean(Integer.toString(i + 1000)).setName("test" + i).setWeight(RandomUtils.nextLong(0, 100));
            dynamap.save(new SaveParams<>(doc).withSuffix(suffix));
            DummyDocBean savedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i)).withSuffix(suffix)));
            Assert.assertEquals(savedDoc.getId(), Integer.toString(i));
        });

        // batch get
        List<GetObjectRequest<DummyDocBean>> getObjectRequests = IntStream.range(0, MAX).mapToObj(i -> {
            String suffix = "-" + i;
            return new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(Integer.toString(i + 1000)).withSuffix(suffix);
        }).collect(Collectors.toList());

        BatchGetObjectParams<DummyDocBean> batchRequest = new BatchGetObjectParams<DummyDocBean>().withGetObjectRequests(getObjectRequests);
        List<DummyDocBean> dummyDocBeans = dynamap.batchGetObjectSingleCollection(batchRequest);
        Assert.assertEquals(dummyDocBeans.size(), MAX);

        // updates
        DummyDocBean dummyDocBean = dummyDocBeans.get(0);
        String docId = dummyDocBean.getId();

        DummyDocUpdates docUpdates = dummyDocBean.createUpdates();
        String updatedName = "updated name";
        docUpdates.setName(updatedName);
        // deduct table suffix by the id
        String suffix = "-" + (Integer.valueOf(docId) % 10);
        dynamap.update(new UpdateParams<>(docUpdates).withSuffix(suffix));

        DummyDocBean updatedDoc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix)));
        Assert.assertEquals(updatedDoc.getId(), docId);

        //batch save
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        final int DUMMY_DOCS_SIZE = 9;
        List<String> dummyDocsIds = new ArrayList<>();
        for (long i = 0; i < DUMMY_DOCS_SIZE; i++) {
            String id = UUID.randomUUID().toString();
            DummyDocBean doc = new DummyDocBean(id).setName("name" + i).setWeight(i);

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
        DummyDocBean doc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix)));
        Assert.assertNotNull(doc);
        DeleteRequest<DummyDocBean> deleteRequest = new DeleteRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix);
        dynamap.delete(deleteRequest);
        doc = dynamap.getObject(new GetObjectParams<>(new GetObjectRequest<>(DummyDocBean.class).withHashKeyValue(docId).withSuffix(suffix)));
        Assert.assertNull(doc);
    }

    @Test
    public void testLocalSecondaryIndex() {
        final int DUMMY_DOCS_SIZE = 13;
        List<DynamapRecordBean> docsToSave = new ArrayList<>();
        List<String> dummyDocsIds = new ArrayList<>();

        String id = "123";
        for (int i = 0; i < DUMMY_DOCS_SIZE; i++) {
            DummyDoc2Bean doc = new DummyDoc2Bean(id, "name" + i).setWeight(i);
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
        doc.setSetOfString(ImmutableSet.of("a"));
        doc.setString("string1");
        doc.setMapOfLong(ImmutableMap.of("a", 1L, "b", 1L));
        doc.setIntegerField(1);
        doc.setListOfInteger(Arrays.asList(1));
        dynamap.save(new SaveParams<>(doc));

        NestedTypeUpdates nestedTypeUpdates = nested.createUpdates();
        TestDocumentUpdates testDocumentUpdates = doc.createUpdates().setNestedObjectUpdates(nestedTypeUpdates);
        testDocumentUpdates.setSetOfStringItem("b");
        testDocumentUpdates.setString("string2");
        testDocumentUpdates.incrementMapOfLongAmount("a", 1L);
        testDocumentUpdates.addListOfIntegerValue(2);
        testDocumentUpdates.setNotPersistedString("notpersisted");
        testDocumentUpdates.setNotPersistedMapOfLongValue("notpersisted", 1L);
        nestedTypeUpdates.setString("string2");
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);

        TestDocumentUpdateResult updated = dynamap.update(new UpdateParams<>(testDocumentUpdates).withReturnValue(DynamapReturnValue.UPDATED_NEW));
        Assert.assertEquals(updated.getIntegerField().intValue(), 1);
        Assert.assertTrue(updated.getSetOfString().contains("a"));
        Assert.assertTrue(updated.getSetOfString().contains("b"));
        Assert.assertTrue(updated.wasSetOfStringUpdated());
        Assert.assertEquals(updated.getMapOfLong().get("a").longValue(), 2L);
        Assert.assertEquals(updated.getMapOfLong().get("b").longValue(), 1L);
        Assert.assertEquals(updated.getListOfInteger().size(), 2);
        Assert.assertEquals(updated.getString(), "string2");
        Assert.assertNull(updated.getNotPersistedString());
        Assert.assertEquals(updated.getNotPersistedMapOfLong().size(), 0);
        Assert.assertTrue(updated.getNestedObjectUpdateResult().wasStringUpdated());
        Assert.assertEquals(updated.getNestedObject().getString(), "string2");
        Assert.assertEquals(updated.getNestedObject().getIntegerField().intValue(), 1);


        testDocumentUpdates = new TestDocumentBean(updated).createUpdates();
        testDocumentUpdates.incrementIntegerField(1);
        nestedTypeUpdates = testDocumentUpdates.getNestedObject().createUpdates();
        testDocumentUpdates.setNestedObjectUpdates(nestedTypeUpdates);
        nestedTypeUpdates.incrementIntegerField(1);
        testDocumentUpdates.deleteMapOfLongValue("a");


        TestDocumentUpdateResult result = dynamap.update(new UpdateParams<>(testDocumentUpdates).withReturnValue(DynamapReturnValue.UPDATED_NEW));
        Assert.assertEquals(result.wasStringUpdated(), false);
        Assert.assertEquals(result.getIntegerField(), new Integer(2));
        Assert.assertEquals(result.getMapOfLong().size(), 1);
        Assert.assertTrue(result.wasMapOfLongUpdated());

        // Test Updates Update Result
        TestDocumentUpdatesUpdateResult testDocumentUpdatesUpdateResult = new TestDocumentUpdatesUpdateResult(result);
        Assert.assertEquals(testDocumentUpdatesUpdateResult.wasStringUpdated(), false);
        // Check that modifying a value after update makes it appear as though it was updated previously
        testDocumentUpdatesUpdateResult.setString("new");
        Assert.assertEquals(testDocumentUpdatesUpdateResult.wasStringUpdated(), true);


        // Test update result when empty set is returned as the new value - this was causing a null pointer exception
        testDocumentUpdates = result.createUpdates();
        testDocumentUpdates.deleteSetOfStringItem("a");
        dynamap.update(new UpdateParams<>(testDocumentUpdates).withReturnValue(DynamapReturnValue.UPDATED_NEW));

    }

    int seq = 0;
    private TestDocumentBean createTestDocumentBean(NestedTypeBean nestedTypeBean) {
        return createTestDocumentBean(UUID.randomUUID().toString(), nestedTypeBean);
    }

    private TestDocumentBean createTestDocumentBean(String id, NestedTypeBean nestedTypeBean) {
        return new TestDocumentBean(id, seq++).setNestedObject(nestedTypeBean);
    }

    private NestedTypeBean createNestedTypeBean() {
        return new NestedTypeBean().setId(UUID.randomUUID().toString());
    }

    private GetObjectParams<TestDocumentBean> createGetObjectParams(TestDocumentBean testDocument) {
        return new GetObjectParams<>(new GetObjectRequest<>(TestDocumentBean.class).withHashKeyValue(testDocument.getId()).withRangeKeyValue(testDocument.getRangeKeyValue()));
    }


}
