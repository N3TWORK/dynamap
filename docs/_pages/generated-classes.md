---
title: "Generated Classes"
permalink: /generated-classes/
---

For each table Dynamap *type*, defined by the [Type](/schema#type-definition) in the schemas, it will generate 3 java classes using the package specified for the table.
For example, for a type named `User` it will generate the following java classes:

Type | Classname 
---|---
[Interface](#interface) | `User` | An interface that specifies the getter methods for the type.
[Bean](#bean-class) | `UserBean` | A bean class that implements the interface. It serves to hold the current state of the object.
[Updates](#updates-class) | `UserUpdates` | A class that implements the interface and provides additional methods for mutating the state and tracking the changes.

## Interface

The interface serves as the top level specification for the type and defines the getter methods for the properties. The other classes implement this interface. It also exposes the DynamoDB named of each fields as constants. These can be used in methods for building conditional expressions.
The interface can be useful in passing an immutable object.

## Bean class

The bean class serves as the container for holding current state. In addition to the getter methods is also provides setter method to set state. The setter methods return the object instance so they can be changed together.
It provides a `createUpdates()` method that will return an `Updates` object that wraps the current state.

```java
UserBean user = new UserBean("id1").setUserName("mark");
UserUpdates = user.createUpdates(); // create the updates object for making changes
```

The class has annotations so that it can easily be serialized to and from JSON using [Jackson](https://github.com/FasterXML/jackson)

## Updates class

The Updates class is used to mutate the state of an object. It wraps an instance of a bean object to hold the current state and then has additional properties to track state changes to that object.
Different methods are provided according to the field types of the object, depending on whether the field is a Collection or a single object, and whether the field type is numeric or a collection of numeric values.
The getter methods return the new state by aggregating the current values with their deltas.

The following methods are provided on every Updates object:

Method | 
---|---
isModified() | returns `true` if there are any changes to the object
getCurrentState() | returns the original unmodified object

The following methods are provided for every field:

Method | 
---|---
set*XXX*() | Sets the value of the field
is*XXX*Modified() | returns `true` if the field has been modified

The following methods are provided for numeric fields:

Method | 
---|---
increment*XXX*(`Type` amount) | Increments the value of the field by `amount`
decrement*XXX*(`Type` amount) | Decrements the value of the field by `amount`

The following methods are provided for Set fields:

Method |
---|---
add*XXX*Item(String item) | Adds the string to the set
clear*XXX*() | Clears the set

The following methods are provided for List fields:

Method | Element Type 
---|---
add*XXX*Amount(`Type` amount) | Numeric | Adds the value to the list
add*XXX*Item(`Type` item) | Non numeric | Adds the item to the list
clear*XXX*() | All | Clears the list

The following methods are provided for Map fields:

Method | Element Type 
---|---
set*XXX*Amount(String key, `Type` amount) | Numeric | Sets the element to the amount
increment*XXX*Amount(String key, `Type` amount) | Numeric | Increments the element by the the amount
decrement*XXX*Amount(String key, `Type` amount) | Numeric | Decrements the element by the the amount
get*XXX*Amount(String key) | Numeric | Returns the current amount for the element
set*XXX*Item(String key, `Type` item) | Non numeric | Sets the element to the given item
get*XXX*Item(String key) | Non numeric | Returns the current item for the specified key
clear*XXX*() | All | Clears the map









