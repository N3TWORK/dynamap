---
title: "Loading Objects"
permalink: /loading-objects/
---

Dynamap provides a few methods for loading an object or multiple objects in a batch. All these methods use the `GetObjectRequest` class to specify what to retreive from DynamoDB.
These objects indicate the type of the objects to retrieve, the key values and whether to use consistent reads.
All these methods also optionally accept read and write rate limiters and a migration context for schema migrations.

The GetObjectRequest object is instantiated with the `Bean` class of the result to be returned.

## Loading a Single Object

Use `getObject()` to load a single object

```java
UserBean user = dynamap.getObject(new GetObjectParams(
            new GetObjectRequest<>(UserBean.class)
                .withHashKeyValue("userId1")
                .consistenRead(true)));
```

## Loading multiple objects of the same type

If your batch get object request will be returning results of the same type (all from the same DynamoDB collection) then use the `batchGetObjectSingleCollection` method.

```java
List<UserBean> userBeans = dynamap.batchGetObject(
                    new BatchGetObjectParams<UserBean>()
                                .withGetObjectRequests(ImmutableList.of(
                                        new GetObjectRequest<>(UserBean.class).withHashKeyValue("userId1"),
                                        new GetObjectRequest<>(UserBean.class).withHashKeyValue("userId2))));
```

## Loading multiple objects of different types

Use the BatchGetObject to load multiple objects with a batch call. This call allows different result types (i.e from different DynamoDB collections).
It returns a Map of List objects where each entry in the map contains all the results for a DynamoDB collection. The key is the Class of the element type.

```java
Map<Class,List<Object>> results = dynamap.batchGetObject(
                    new BatchGetObjectParams()
                          .withGetObjectRequests(ImmutableList.of(
                                  new GetObjectRequest<>(UserBean.class).withHashKeyValue("userId1"),
                                  new GetObjectRequest<>(SomeOtherBean.class).withHashKeyValue("anotherId")));
```
