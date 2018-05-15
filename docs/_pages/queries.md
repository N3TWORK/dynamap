---
title: "Queries"
permalink: /queries/
---

Dynamap's `query` method provides the ability to execute all sorts of queries. The `QueryRequest` object is used to specify the hash key value and an optional range key condition.
You can also instruct the query to use a local or global secondary index if they have been defined in the schema.
The range key condition is specified using the RangeKeyCondition class directly from the DynamoDB API.

This example shows how to query a range of objects for a hash key using a global secondary index.

```java
QueryRequest<TestDocumentBean> queryRequest = new QueryRequest<>(TestDocumentBean.class)
                .withHashKeyValue(doc.getString())
                .withRangeKeyCondition(
                        new RangeKeyCondition(TestDocumentBean.INTEGERFIELD_FIELD).eq(doc.getIntegerField()))
                .withIndex(TestDocumentBean.GlobalSecondaryIndex.testIndexFull);

List<TestDocumentBean> testDocuments = dynamap.query(queryRequest);
```


