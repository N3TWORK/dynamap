---
title: "Updating Objects"
permalink: /updating-objects/
---

Dynamap generates a class that implements the `Updates` interface for making changes to the object. It exposes various methods that are used to make changes and tracks these deltas.
When it is time to persist the changes to the database Dynamap uses this object to generate update and conditional expressions in order to execute the update.
The updates object is generated from the bean that is to be mutated. 

## Obtaining the updates object

To create an object for mutating a bean, call the `createUpdates()` method.

```java
UserUpdates userUpdates = exsitingUserBean.createUpdates();
```

It is also possible to perform ***blind*** updates, that is, making an update to state without having to read it first. To do this simply create a new instance of the bean with the key.
The updates is smart enough to only update the fields that have changed.

```java
UserUpdates userUpdates = userBean.createUpdates(new UserBean("userId1"));
```

## Update methods

For a full list of the types of update methods provides consult [Generated classes](/generated-classes/).

### Setting a value

SetXXX methods allow you to set the field.

```java
Map<String,Long> balances = new HashMap();
balances.put("gold", 2);
balances.put("silver", 2);
bean.incrementCurrencyBalances(balances);
```

### Setting an individual item in a map

You can set an individual item in a map. This will result in a "SET" operation in the update expression that sets the value of the specific item, rather than writing back all values in the map.
Thus these can be used safely for blind updates.

```java
bean.setCurrencyBalanceValue("gold", 3);
```


### Incrementing a integer value in a map

Using increment/decrement method result in an "ADD" operation in DynamoDB. Thus these can be used safely for blind updates and are also safe with concurrent updates.

```java
bean.incrementCurrencyBalanceValue("gold", 2);
```

### Checking if there are pending updates

The updates objects provide a mechanism to check if there are any pending updates or even if a particular field has been modified

```java
if (bean.isModified()) {
    // do update
} 

if (bean.isCurrencyBalancesModified()) {
    // do something
}
```

## Making the call to update

Dynamap provides a single `update` method with an `UpdateParams` object. The UpdateParams requires the updates object and accepts additional optional settings such as write limiters and the type of return value (all updates field, just updated field etc).  
NOTE: Putting too many modifications in a single update might result in an error from DynamoDB about expression size exceeded. If you have around 50 or more updates to make, do them in separate update calls.  

```java
User updatedUser = dynamap.update(new UpdateParams(userUpdates));
```

## Returning only Updates Values from Updates

The `UpdateParams.withReturnValue()` allows you to indicate which values should be returned, either all or only updates, new or old.
However, Dynamap always returns a full populated object regardless of which option was specified. It does this by returning the object in an UpdateResult bean class. This class can determine which values were actually updated and fall back to old values to give the appearance of a fully updated object.
It also provides additional methods that indicate which fields were actually updated.

**Note**: if `ALL_OLD` or `ALL_NEW` is used then the checks for fields being updated will return true regardless of whether they were actually updated or not.

```java
userUpdates.incrementCurrencyBalanceValue("gold", 2);
UserUpdateResult updatedUser = dynamap.update(new UpdateParams(userUpdates).withReturnValue(DynamapReturnValue.UPDATED_NEW));
assert updatedUser.wasCurrencyBalancesUpdates();
```
