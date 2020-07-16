---
title: "Schema Definition"
permalink: /schema/
---
## Structure of a Schema File

A schema file is a JSON object that contains one or more table definitions. A *table* corresponds to a DynamoDB *collection*.
You may find it easier to manage multiple tables if you split up your table definitions among multiple files -- one table definition per file.

The table definition contains attributes that define the collection, such as the hash key, optional range key and the object type of the document.
The rest of the schema definition contains the type definition of the document and additional definitions if the document contains nested types.

## Top Level Definition

Attribute | Required | Description
--- | ---
`tables` | yes | An array of table definitions

## Table Definition

Attribute | Required | Description
--- | ---
`table` | yes | The name of the DynamoDB collection
`package` | yes | The Java package for the generated classes
`type` | yes | The object type of the DynamoDB document. This type must be defined in the [type](#type-definition) section.
`version` | yes | The version number of the schema.
`hashKey` | yes | The name of the field (not the DynamoDB field name) which represents the hash key of the collection.
`rangeKey` | no | The name of the field which presents the range key of the collection.
`globalSecondaryIndexes` | no |  An array of one or more global secondary index definitions
`optimisticLocking` | no | boolean: `true` or `false`, default: `false`. If `true` adds a revision field and conditional checks to updates to implement optimistic locks.
`types` | yes | An array of one or more ***[type](#type-definition)*** definitions. This must include at least one entry - the definition for the type of the collection.

## Global Secondary Index Definition

Attribute | Required |  Description
--- | ---
`index` | yes | The name global secondary index
`hashKey` | yes | The name of the field to use as the hash key
`rangeKey` | no | The name of the field to use as the range key
`nonKeyFields` | no | An array of the non key fields to include. This is only necessary if using the projection type ***INCLUDE***.
`projectionType` | no | The projection type to indicate which non key fields to include. This corresponds to the DynamoDB projection type enumerator values: ***ALL***,***KEYS_ONLY***,***INCLUDE***. The default value if not specified is ***ALL***. 

## Type Definition

Attribute | Required |  Description
--- | ---
`name` | yes | The object type name. This is used as the name of the Java interface and prefix for the associated implementation Java class names.
`description` | no | A description of the type. This is for your documentation purposes only.
`hashCodeFields` | no | An array of field names to use in the hashCode() method of the generated Java classes.
`equalsFields` | no | An array of field names to use in the equals() method of the generated Java classes.
`fields` | yes | An array of ***[field](#field-definition)*** definitions. This will at a minimum include the hashCode and rangeKey fields.

## Field Definition

Attribute | Required |  Description
--- | ---
`name` | yes | The object field name. This is used as the name of the property in the Java classes.
`description` | no | A description of the field. This is for your documentation purposes only.
`dynamoName` | yes | The name of the field or property when serialized and persisted in DynamoDB.
`type` | yes | The object class type. The following types are permitted:<br/><br/>* *Java primitives*: **String**,**Integer**,**Long**,**Float**,**Double**<br/>* *Collections*: **Map**,**List**,**Set**<br/>* *Dynamap Types*: Any Dynamap generated [type](#type-definition) defined for this table.<br/>* *Custom classes*: Any fully qualified class name<br/>
`elementType` | no | Required for collection types. Specifies for element type of the collection
`default` | no | A Java expression that returns the default value.
`useDefaultForNulls` | no | Only relevant when type is **Map**. For get methods, returns the default value when an element does not exist.
`replace` | no | boolean: `true` or `false`, default `false`. Only relevant for collection types. When `true` Dynamap will replace the entire collection wholesale during updates rather than using an update expressions. This maybe necessary for large collections which could result in the size of the entire update expression exceeding DynamoDB's limit. The downside of this setting is that individual items changes are no longer concurrently safe - stale values may overwrite an update from another concurrent process.
`persist` | no | boolean: `true` or `false`, default `true`. When `false` Dynamap will not read or write the data from DyanmoDB. This might be useful if you want to track ephemeral state on the *Updates* object during a request but not have persist to the database.
`deltas` | no | boolean: `true` or `false`, default `true`. When `false` Dynamap will not track deltas. The *Updates* object will only expose methods for setting the value or entire collection. For numeric values there will be no increment or decrement methods.
`compressCollection` | no | string: `gzip`. When set Dynamap will compress the entire collection and serialize it as a binary type using the compression method. Currently only `gzip` is supported. Note that as the entire collection is compressed this has the same effect as using `replace`, i.e. fine grained updates are not possible and so concurrent operations are not safe.
`serializeAsListElementId` | no | A common use case is to use maps as an index to a collection of unique beans. This setting allows the map to be serialized as a list and then re-constructed as a map by deriving the map's key from the property of the bean specified. This results in a more efficient storage representation and much better compression if compression is enabled. Note that the property corresponds to the bean's field as it is serialized. i.e., the Jackson annotation if using a custom provided class or the `dynamoName` of using a Dynamap defined type.

### Example Schema

Below is the schema used for the unit tests. It demonstrates the full functionality.

```json
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
            "listOfString"
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
              "name": "listOfString",
              "dynamoName": "strList",
              "type": "List",
              "elementType": "String"
            },
            {
              "name": "listOfInteger",
              "dynamoName": "intList",
              "type": "List",
              "elementType": "Integer"
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
