---
title: "Using Rate Limiters"
permalink: /rate-limiters
---

DynamoDB will throw an exception if read or write capacity is exceeded.
Dynamap provides the `DynamoRateLimiter` class for rate limiting reads and writes to DynamoDB which is useful if you are performing multiple reads and writes in a loop (for example, a bulk process) or when performing large batch reads.

The rate limiter is created by specifing a target capacity which is a percentage of the total capacity allocated to the table. Dynamap will attempt to ensure that no more than the target capacity is used.
For example, if the DynamoDB collection has 100 read units allocated and the target capacity of the rate limiter is 20, then Dynamap will attempt to ensure that no more than 20 units are consumed.

Instantiating the rate limiters:

```java
DynamoRateLimiter writeRateLimiter =  new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 30)
User userBean = new UserBean().setId("id1");
dynamap.save(new SaveParams(userBean).withWriteLimiter(writeRateLimiter));
```

Some methods require both a read and write rate limiter be supplied and these are passed in using the `ReadWriteRateLimiterPair` class.

```java
ReadWriteRateLimiterPair rateLimiterPair = ReadWriteRateLimiterPair.of(
        new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.READ, 20),
        new DynamoRateLimiter(DynamoRateLimiter.RateLimitType.WRITE, 30));

UserBean user = dynamap.getObject(new GetObjectParams(
            new GetObjectRequest<>(UserBean.class)
                .withHashKeyValue("id1"))
             .withRateLimiters(rateLimiterPair));
```

**Note:**
 
Creating a rate limiter for each request would have no effect since the purpose is to provide rate limiting across many concurrent requests for the Java process.
RateLimiters are thread safe and so are usually created once in another method and retained for the lifetime of the application and shared globally by multiple concurrent threads.
RateLimiters can only provide rate limiting for a single Java process. If you have multiple Java processes making DynamoDB calls then you would adjust the target capacity to take this into consideration.
