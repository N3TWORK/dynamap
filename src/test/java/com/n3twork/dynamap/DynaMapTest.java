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
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.n3twork.dynamap.test.ExampleDocument;
import com.n3twork.dynamap.test.ExampleDocumentBean;
import com.n3twork.dynamap.test.NestedTypeBean;
import com.n3twork.dynamap.test.NestedTypeUpdates;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.UUID;

public class DynaMapTest {

    private AmazonDynamoDB ddb;

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeTest
    public void init() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        ddb = DynamoDBEmbedded.create().amazonDynamoDB();
    }

    @Test
    public void testDynaMap() {
        SchemaRegistry schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json"));
        DynaMap dynaMap = new DynaMap(ddb, "test", schemaRegistry, objectMapper);
        dynaMap.createTables(true);
        String exampleId = UUID.randomUUID().toString();
        String nestedId = UUID.randomUUID().toString();

        NestedTypeBean nestedObject = new NestedTypeBean(nestedId, null, null, null, null, null,
                null, null, null, null);
        ExampleDocumentBean doc = new ExampleDocumentBean(exampleId,
                1, nestedObject, null, null, "alias");

        dynaMap.save(doc, null);

        QueryRequest<ExampleDocument> queryRequest = new QueryRequest(ExampleDocumentBean.class).withHashKeyValue(exampleId).withRangeKeyValue(1);
        ExampleDocument exampleDocument = dynaMap.query(queryRequest, null);

        Assert.assertEquals(exampleDocument.getId(), exampleId);
        nestedObject = new NestedTypeBean(exampleDocument.getNestedObject());
        Assert.assertEquals(nestedObject.getId(), nestedId);

        NestedTypeUpdates nestedTypeUpdates = new NestedTypeUpdates(nestedObject, objectMapper, exampleId, 1);
        nestedTypeUpdates.setBio("test");
        dynaMap.update(nestedTypeUpdates);

        exampleDocument = dynaMap.query(queryRequest, null);
        Assert.assertEquals(exampleDocument.getNestedObject().getBio(), "test");


    }


}
