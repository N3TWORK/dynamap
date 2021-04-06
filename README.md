# Dynamap

Dynamap is Java object model and mapping library for Amazon's DynamoDB database.

It generates strongly typed Java classes that represent your schema and indexes, and provides methods for saving, querying and updating the state.

**Creating and persisting an object is as simple as:**
```java
 UserBean user = new UserBean("mark").setCurrencyBalancesAmount("gold",10);
 dynamap.save(new SaveParams(user));
```

**Reading an object:**

```java
UserBean user = dynamap.getObject(new GetObjectParams(new GetObjectRequest<>(UserBean.class).withHashKeyValue("mark")));
```

**Updating an object:**
```java
UserBeanUpdates updates = user.createUpdates()
updates.incrementCurrencyBalancesAmount(2).setStatus("away");
dynamap.update(new UpdateParams(updates));
```

**Benefits**:

* Define your schema and attribute behavior using JSON.
* Strongly typed classes are automatically generated.
* Automatically creates collections and indexes in DynamoDB.
* Update objects hide the complexity of DynamoDB update expressions and provide the ability to execute fine grained, concurrently safe updates.
* Updates track changes to the state of the object, including delta amounts for numeric types. Original state can be retrieved if necessary.
* Supports blind updates - the ability to modify persisted state without having to read it first.
* Provides simple methods that hide the complexity of building conditional expressions.
* Significantly reduces the amount of code you have to write and makes your code easy to read and comprehend.
* Provides a mechanism for rate limiting reads and writes.
* Additional custom generated types can be defined and nested in the top level document.
* Easy to make simple modifications to your schema such as adding new fields. Just make the change and regenerate the code
* Provides a mechanism for schema migrations for more complex changes

## Quick Start

[Check out the quick start guide](https://dynamap.n3twork.com/quickstart)

or consult the full documentation on the official site: https://dynamap.n3twork.com

## Available on Maven Central

Dynamap is published on Maven central

```xml
<dependency>
    <groupId>com.n3twork.dynamap</groupId>
    <artifactId>dynamap</artifactId>
    <version>0.9.65</version>
</dependency>
```
