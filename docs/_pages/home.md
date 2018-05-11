---
permalink: /
---

Dynamap is Java object model and mapping library for Amazon's DynamoDB database.

It automatically generates strongly typed Java classes from a schema that you define, and provides methods for saving, querying and updating the state.

In addition to standard CRUD operations, it provides many more features such as tracking state changes, indexes, optimistic locking, a mechanism for performing schema migrations, read and write rate limiting and more. 
 

**Creating and persisting an object:**
```java
 UserBean user = new UserBean().setId("mark").setCurrencyBalancesAmount("gold",10);
 dynamap.save(user);
```

**Reading an object:**

```java
UserBean user = dynamap.getObject(new GetObjectRequest<>(UserBean.class).withHashKeyValue("mark"));
```

**Updating an object:**
```java
UserBeanUpdates updates = new UserBeanUpdates(new User(),"mark"); // supports blind updates
updates.incrementCurrencyBalancesAmount(2).setStatus("away");
dynamap.update(new UpdateParams(updates));
```
