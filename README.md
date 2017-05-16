# Dynamap

[![](https://jitpack.io/v/com.n3twork/dynamap.svg)](https://jitpack.io/#com.n3twork/dynamap)

A Java object mapping library for Amazon's DynamoDB database.

Generates strongly typed Java classes that represent your schema, and provides methods for saving, querying and updating the state.

Benefits:

* Define your schema using JSON.
* Strongly typed classes are automatically generated.
* Update objects hide the complexity of DynamoDB update expressions and provide the ability to execute fine grained, concurrently safe updates.
* Provides a mechanism for schema migrations
* Provides a mechanism for rate limiting reads and writes
* Additional custom generated types and can be defined and nested in the top level document


## Overview

### Define a schema


Using this simple schema:

```javascript

{
  "tables": [
    {
      "table": "User",
      "package": "com.n3twork.dynamap.example",
      "type": "User",
      "version": 1,
      "hashkey": "id",
      "types": [
        {
          "name": "User",
          "fields": [
            {
              "name": "id",
              "dynamoName": "id",
              "type": "String"
            },
            {
              "name": "userName",
              "dynamoName": "un",
              "type": "String"
            },
            {
              "name": "balances",
              "dynamoName": "bl",
              "type": "Long",
              "multivalue": "Map"
            }
          ]
        }
      ]
    }
  ]
}


```

Dynamap will generate the following 3 java classes:

* `com.n3twork.dynamp.example.User` : an interface defining the attributes of the table record
* `com.n3twork.dynamp.example.UserBean` : a bean class that implements the interface and is used for holding the data
* `com.n3twork.dynamp.example.UserUpdates` : a class that extends the bean class and provides additional methods for mutating the data. It wraps the original bean class and thus retains old and new state


### Write code to create tables, store, query and update

First create an instance of Dynamap, providing the handle to DynamoDB and a pointer to the schema file:

```
 AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB(); // use Local DynamoDB library
 SchemaRegistry schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/TestSchema.json")); // load schema from schema file
 Dynamap dynamap = new Dynamap(ddb, "test", schemaRegistry, new ObjectMapper()); // create Dynamap and use "test" as a prefix for the tables
 dynamap.createTables(false); // create tables, do not delete if they already exist
     
```
    
Create some new data using the generated bean class and save it

```java

 Map<String,Long> balances = new HashMap();
 balances.put("gold", 2);
 balances.put("silver", 2);
 UserBean user = new UserBean("userId1", "mark", balances);
 dynamap.save(user);
 
```

Get the user object

```java
 GetObjectRequest<User> getObjectRequest = new GetObjectRequest(UserBean.class).withHashKeyValue("userId1");
 User user = dynamap.getObject(getObjectRequest);
```
 
Update the user, by incrementing a balance

```java
UserUpdates updates = new UserUpdates(user, new ObjectMapper(), "userId1");
updates.incrementBalanceAmount("gold",3);
dynamap.update(updates);

```



## Getting Started

In your Maven project file add the following repository:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

```

Add the following dependency: 

```xml
<dependency>
    <groupId>com.n3twork</groupId>
    <artifactId>dynamap</artifactId>
    <version>-SNAPSHOT</version>
</dependency>
```

Execute the code generator on your your schema file and bind this execution to the `generate-sources` Maven build phase:

```xml
  <plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <executions>
        <execution>
            <id>generate-source-files</id>
            <phase>generate-sources</phase>
            <goals>
                <goal>java</goal>
            </goals>
            <configuration>
                <mainClass>com.n3twork.dynamap.CodeGenerator</mainClass>
                <arguments>
                    <argument>--schema</argument>
                    <argument>${project.basedir}/src/main/resources/DynamoDBSchema.json</argument>
                    <argument>--output</argument>
                    <argument>${project.build.directory}/generated-sources</argument>
                </arguments>
            </configuration>
        </execution>
    </executions>
  </plugin>


```

The maven-build-helper can add these generated sources to your classpath:

```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>build-helper-maven-plugin</artifactId>
                <version>1.8</version>
                <executions>
                    <execution>
                        <id>add-source</id>
                        <phase>generate-test-sources</phase>
                        <goals>
                            <goal>add-source</goal>
                        </goals>
                        <configuration>
                            <sources>
                                <source>generated-sources</source>
                            </sources>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
         </plugins>
     </build>
```


## A more complex schema

In addition to defining a class that maps to the DynamoDB, additional classes can be defined that are nested within the top level class. Each of these types have their own interface, bean and updates classes automatically generated.

The following is a full schema that is used by the unit tests:

```javascript

{
  "tables": [
    {
      "table": "Example",
      "package": "com.n3twork.dynamap.test",
      "type": "ExampleDocument",
      "version": 1,
      "hashkey": "id",
      "rangekey": "range",
      "globalSecondaryIndexes": [
        {
          "index": "exampleIndex",
          "hashKey": "alias",
          "rangeKey": "range"
        }
      ],
      "types": [
        {
          "name": "ExampleDocument",
          "fields": [
            {
              "name": "id",
              "dynamoName": "id",
              "type": "String"
            },
            {
              "name": "range",
              "dynamoName": "range",
              "type": "Integer"
            },
            {
              "name": "nestedObject",
              "dynamoName": "nested",
              "type": "NestedType"
            },
            {
              "name": "mapOfLong",
              "dynamoName": "mapOfLong",
              "type": "Long",
              "multivalue": "Map"
            },
            {
              "name": "mapOfCustomType",
              "dynamoName": "mapOfCustomType",
              "type": "com.n3twork.dynamap.CustomType",
              "multivalue": "Map"
            },
            {
              "name": "alias",
              "dynamoName": "alias",
              "type": "String"
            }
          ]
        },
        {
          "name": "NestedType",
          "fields": [
            {
              "name": "id",
              "dynamoName": "id",
              "type": "String"
            },
            {
              "name": "bio",
              "dynamoName": "bio",
              "type": "String",
              "default": "empty"
            },
            {
              "name": "integerField",
              "dynamoName": "integerField",
              "type": "Integer"
            },
            {
              "name": "mapOfLong",
              "dynamoName": "mapOfLong",
              "type": "Long",
              "multivalue": "Map",
              "useDefaultForMap": "true"
            },
            {
              "name": "setOfLong",
              "dynamoName": "setOfLong",
              "type": "Long",
              "multivalue": "Set"
            },
            {
              "name": "listOfLong",
              "dynamoName": "listOfLong",
              "type": "Long",
              "multivalue": "List"
            },
            {
              "name": "customType",
              "dynamoName": "customType",
              "type": "com.n3twork.dynamap.CustomType"
            },
            {
              "name": "mapOfCustomType",
              "dynamoName": "mapOfBean",
              "type": "com.n3twork.dynamap.CustomType",
              "multivalue": "Map"
            },
            {
              "name": "setOfCustomType",
              "dynamoName": "setOfCustomType",
              "type": "com.n3twork.dynamap.CustomType",
              "multivalue": "Set"
            },
            {
              "name": "listOfCustomType",
              "dynamoName": "listOfCustomType",
              "type": "com.n3twork.dynamap.CustomType",
              "multivalue": "List"
            }
          ]
        }
      ]
    }
  ]
}

```


