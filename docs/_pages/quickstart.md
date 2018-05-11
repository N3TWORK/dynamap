---
title: "Quickstart"
permalink: /quickstart
---
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
              "type": "Map",
              "elementType": "Long"
            }
          ]
        }
      ]
    }
  ]
}
```

### Step 3. Configure the code generator

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

**Optional**: The maven-build-helper can add these generated sources to your classpath:

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

### Step 4. Generate the code

Execute the generate-sources maven goal to trigger the code generation:

```bash
$ mvn generate-sources
```

Dynamap will generate the following 3 java classes:


--- | ---
`com.n3twork.dynamp.example.User` | an interface defining the attributes of the table record
`com.n3twork.dynamp.example.UserBean` | a bean class that implements the interface and is used for holding the data
`com.n3twork.dynamp.example.UserUpdates` | a class that extends the bean class and provides additional methods for mutating the data. It wraps the original bean class and thus retains old and new state


### Step 5. Write code to create tables, store, query and update

First create an instance of Dynamap, providing the handle to DynamoDB and a pointer to the schema file:

```java
 AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB(); // use Local DynamoDB library
 SchemaRegistry schemaRegistry = new SchemaRegistry(getClass().getResourceAsStream("/<your-schema>.json"));
 Dynamap dynamap = new Dynamap(ddb, schemaRegistry);
 dynamap.createTables(false); // create tables, do not delete if they already exist
     
```
    
Create some new data using the generated bean class and save it

```java

 Map<String,Long> balances = new HashMap();
 balances.put("gold", 2);
 balances.put("silver", 2);
 UserBean user = new UserBean().setUserId("userId1")
                               .setUserName("mark")
                               .setBalances(balances);
 dynamap.save(user);
 
```

Get the user object

```java
 GetObjectRequest<User> getObjectRequest = new GetObjectRequest(UserBean.class).withHashKeyValue("userId1");
 User user = dynamap.getObject(getObjectRequest);
```
 
Update the user, by incrementing a balance

```java
UserUpdates updates = new UserUpdates(user, "userId1");
updates.incrementBalanceAmount("gold", 3);
dynamap.update(updates);

```

Increment the user's balance without having to read the existing user object first

```java
UserUpdates updates = new UserUpdates(new UserBean(), "userId1");
updates.incrementBalanceAmount("gold", 3);
dynamap.update(updates);

```