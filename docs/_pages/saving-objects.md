---
title: "Saving Objects"
permalink: /saving-objects
---

Saving objects is very simple with Dynamap using `save` method. It takes a single SaveParams object as a parameter which allows you to specify additional options such as the write rate limiter.
Dynamap understands which fields hold the hash optional range key values, so you don't need to specify them. Just make sure that they have been initialized on the object you are persisting.

The save method executes a DynamoDB `putObject` beneath the hood.
By default save will overwrite any existing document. If you want safer behavior then you can specify `disableOverwrite` on the SaveParams object which will result in an Exception being thrown if a document with the key already exists.

```java
User userBean = new UserBean().setId("id");
dynamap.save(new SaveParams(userBean).withDisableOverwrite(true));
```

