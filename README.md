# simple-persistence
Persistence library that provides common data structures as composable elements to abstract any storage engine, including SQL databases.

## Introduction

Today's applications stores data in different places like the own process memory, SQL databases, Redis and other NoSQL databases. Usually, the developer uses distinct code patterns for each one of these storage engines. For memory storage, primitives and simple data structures are used, like lists and hashes. And for SQL databases, the most common pattern is the repository + unit of work. While this seems the right thing to be done (since you cannot perform advanced queries on a Redis database - for instance), sometimes the developer doesn't needs the specific capabilities of that engine but he will implement the storage engine specific pattern instead of using simple data structures.

For instance, there's no semantic difference between a repository ```GetById``` method and a ```Dictionary``` (hash table) ```TryGetValue``` method. But even if the application only uses the first method, probably the developer will implement something using the repository pattern for SQL data access. But what happens if the persistence layer needs to be moved to Redis or Mongo? Probably, the developer will implement the repository pattern for the target engine, leaving some methods that are not supported empty (like queries on Redis). Or maybe, he will need to refactor the code that uses the storage class...

The idea behind this library is expose a common layer that can be used with multiple storage engines, while isolating the specific capabilities of each one, allowing the developer compose the application storage infrastructure accordingly to the its needs. It starts from the simple and common data structures, expanding according to the capabilities of each target engine. 

## When to use it?

* When you have multiple storage engines in your application
* When you do not need referential integrity (do you really need it?)
* When the data can be denormalized
* When you need good performance

## Data structures

### Primitive structures

Name                 | Description
---------------------|---------------
IMap<TKey, TValue>   | Mapper that provides fast access to a value using a key.
ISet<T>              | Set of unique items.
IQueue<T>            | FIFO storage container.
IQueryableStorage<T> | Storage that supports queries.

### Composite structures

Name                    | Description
------------------------|---------------
ISetMap<TKey, TValue>   | Map that contains a set on unique items.
IQueueMap<TKey, TValue> | Map that contains a queue of items.

### Extended structures

Name                            | Parent               | Description
--------------------------------|----------------------|--------------------------------
IExpirableKeyMap<TKey, TValue>  | IMap<TKey, TValue>   | Map that supports key expiration.
IItemSetMap<TKey, TItem>        | ISetMap<TKey, TValue>   | SetMap that allows to get a specific item in the set.


## Current supported storage engines

* Memory (System.Collections)
* SQL Server
* Redis

 

