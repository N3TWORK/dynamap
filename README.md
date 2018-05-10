# Dynamap

Dynamap is Java object model and mapping library for Amazon's DynamoDB database.

It generates strongly typed Java classes that represent your schema and indexes, and provides methods for saving, querying and updating the state.

**Creating and persisting an object is as simple as:**
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
UserBeanUpdates updates = new UserBeanUpdates(user);
updates.incrementCurrencyBalancesAmount(2).setStatus("away");
dynamap.update(new UpdateParams(updates));
```

**Benefits**:

* Define your schema and attribute behavior using JSON.
* Strongly typed classes are automatically generated.
* Update objects hide the complexity of DynamoDB update expressions and provide the ability to execute fine grained, concurrently safe updates.
* Updates track changes to the state of the object, including delta amounts for numeric types. Original state can be retrieved if necessary.
* Provides simple methods that hide the complexity of building conditional expressions.
* Significantly reduces the amount of code you have to write and makes your code easy to read and comprehend
* Provides a mechanism for schema migrations
* Provides a mechanism for rate limiting reads and writes
* Additional custom generated types can be defined and nested in the top level document



## Quickstart

### Step 1. Add the maven dependency

In your Maven project file add the following dependency:

```xml
<dependency>
    <groupId>com.n3twork</groupId>
    <artifactId>dynamap</artifactId>
    <version>0.9.29</version>
</dependency>
```

### Step 2. Define a schema

Create this simple schema below and save it to a file in your project resources folder.

```javascript
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

Attach the code generator on your schema file and bind this execution to the `generate-sources` Maven build phase:

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
                    <argument>${project.basedir}/src/main/resources/<your-schema-file>.json</argument>
                    <argument>--output</argument>
                    <argument>${project.build.directory}/generated-sources/dynamap/</argument>
                </arguments>
            </configuration>
        </execution>
    </executions>
  </plugin>
```

Optional: The maven-build-helper can add these generated sources to your classpath:

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
                                <source>${project.build.directory}/generated-sources/dynamap/</source>
                            </sources>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
         </plugins>
     </build>
```

### Step 3. Generate the code

Execute the generate-sources maven goal to trigger the code generation:

`mvn generate-sources`

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
updates.incrementBalanceAmount("gold", 3);
dynamap.update(updates);

```

Increment the user's balance without having to read the existing user object first

```java
UserUpdates updates = new UserUpdates(new UserBean(), new ObjectMapper(), "userId1");
updates.incrementBalanceAmount("gold", 3);
dynamap.update(updates);

```

### A more complex schema

In addition to defining a class that maps to the DynamoDB, additional classes can be defined that are nested within the top level class. Each of these types have their own interface, bean and updates classes automatically generated.

The following is a full schema that is used by the unit tests:

```javascript
{
  "tables": [
    {
      "table": "Test",
      "description": "Test Schema",
      "package": "com.n3twork.dynamap.test",
      "type": "TestDocument",
      "version": 1,
      "hashKey": "id",
      "rangeKey": "sequence",
      "globalSecondaryIndexes": [
        {
          "index": "testIndexProjection",
          "hashKey": "string",
          "rangeKey": "integerField",
          "nonKeyFields": [
            "mapOfLong",
            "someList"
          ],
          "projectionType": "INCLUDE"
        },
        {
          "index": "testIndexFull",
          "hashKey": "string",
          "rangeKey": "integerField"
        }
      ],
      "types": [
        {
          "name": "TestDocument",
          "description": "Top level document",
          "hashCodeFields": [
            "id",
            "sequence"
          ],
          "equalsFields": [
            "id",
            "sequence"
          ],
          "fields": [
            {
              "name": "id",
              "description": "Primary key",
              "dynamoName": "id",
              "type": "String"
            },
            {
              "name": "sequence",
              "dynamoName": "seq",
              "type": "Integer"
            },
            {
              "name": "nestedObject",
              "dynamoName": "nested",
              "type": "NestedType"
            },
            {
              "name": "mapOfLong",
              "dynamoName": "mol",
              "type": "Map",
              "elementType": "Long"
            },
            {
              "name": "mapOfCustomType",
              "dynamoName": "mct",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType"
            },
            {
              "name": "mapOfCustomTypeReplace",
              "dynamoName": "mctr",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType",
              "replace": true
            },
            {
              "name": "noDeltaMapOfCustomType",
              "dynamoName": "noDeltaMapOfCT",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType",
              "deltas": false
            },
            {
              "name": "string",
              "dynamoName": "str",
              "type": "String"
            },
            {
              "name": "integerField",
              "dynamoName": "intf",
              "type": "Integer"
            },
            {
              "name": "someList",
              "dynamoName": "someList",
              "type": "List",
              "elementType": "String"
            },
            {
              "name": "notPersistedString",
              "dynamoName": "notPersistedStr",
              "type": "String",
              "persist": false
            },
            {
              "name": "setOfString",
              "dynamoName": "setOfString",
              "type": "Set",
              "elementType": "String"
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
              "name": "string",
              "dynamoName": "str",
              "type": "String"
            },
            {
              "name": "integerField",
              "dynamoName": "integerField",
              "type": "Integer"
            },
            {
              "name": "notPersistedString",
              "dynamoName": "notPersistedStr",
              "type": "String",
              "persist": false
            },
            {
              "name": "mapOfLong",
              "dynamoName": "mapOfLong",
              "type": "Map",
              "elementType": "Long"
            },
            {
              "name": "mapOfLongWithDefaults",
              "dynamoName": "mapOfLongWithDefaults",
              "type": "Map",
              "elementType": "Long",
              "useDefaultForNulls": "true",
              "default": "2L"
            },
            {
              "name": "mapOfDoubleWithDefaults",
              "dynamoName": "mapOfDoubleWithDefaults",
              "type": "Map",
              "elementType": "Double",
              "useDefaultForNulls": "true",
              "default": "0.0"
            },
            {
              "name": "setOfLong",
              "dynamoName": "setOfLong",
              "type": "Set",
              "elementType": "Long"
            },
            {
              "name": "setOfString",
              "dynamoName": "setOfString",
              "type": "Set",
              "elementType": "String"
            },
            {
              "name": "listOfLong",
              "dynamoName": "listOfLong",
              "type": "List",
              "elementType": "Long"
            },
            {
              "name": "customType",
              "dynamoName": "customType",
              "type": "com.n3twork.dynamap.CustomType"
            },
            {
              "name": "mapOfCustomType",
              "dynamoName": "mct",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType"
            },
            {
              "name": "mapOfCustomTypeReplace",
              "dynamoName": "mctr",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType",
              "replace": true
            },
            {
              "name": "noDeltaMapOfCustomType",
              "dynamoName": "noDeltaMapOfCT",
              "type": "Map",
              "elementType": "com.n3twork.dynamap.CustomType",
              "deltas": false
            },
            {
              "name": "setOfCustomType",
              "dynamoName": "setOfCustomType",
              "type": "Set",
              "elementType": "com.n3twork.dynamap.CustomType"
            },
            {
              "name": "listOfCustomType",
              "dynamoName": "listOfCustomType",
              "type": "List",
              "elementType": "com.n3twork.dynamap.CustomType"
            }
          ]
        }
      ]
    }
  ]
}
```


