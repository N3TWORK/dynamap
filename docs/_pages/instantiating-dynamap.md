---
title: "Instantiating Dynamap"
permalink: /instantiating-dynamap/
---

Dynamap provides a single class, `com.n3twork.Dynamap`, for all interactions with DyanamoDB. It requires two parameters to be instantianted; an instance of the DynamoDB client and the schema registry.

Since AWS does not provide a mechanism for namespacing DynamoDB collections within a single account, Dynamap allows you to specify a prefix to be used with all tables. This is useful if you want to have multiple instances of the same collection, for example if you have development and production collections in the same account.

Dynamap uses  [Jackson](https://github.com/FasterXML/jackson) extensively for serialization to and from json. You can optionally supply your own ObjectMapper to Dynamap in case you have some customizations.

The SchemaRegistry contains all the schema definitions and any migration code (see [schema migrations](/schema-migrations)). It is instantiated by providing a list of InputStream objects.

This is how you might instantiate Dynamp, prefixing all your collections with `prod`. Note the actual name of the collection is `prod.<collection>`

```java

ObjectMapper customObjectMapper = new ObjectMapper();

SchemaRegistry schemaRegistry = new SchemaRegistry(
        getClass().getResourceAsStream("/<your-schema>.json"),
        getClass().getResourceAsStream("/<another-schema>.json"));

Dynamap dynamap = new Dynamap(new AmazonDynamoDBClient(),schemaRegistry)
          .withObjectMapper(customObjectMapper)
          .withPrefix("prod");

```

## Creating Tables and Indexes

Dynamap can automatically create your tables and indexes for you, optionally overwriting existing definitions.

```java
dynamap.createTables(false); // creates the tables but does not overwrite
```