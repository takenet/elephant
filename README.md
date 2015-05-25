# Elephant
Persistence library that provides common data structures as composable elements to abstract any storage engine, including SQL databases.

## Introduction

Today's applications stores data in different places like the own process memory, SQL databases, Redis and other NoSQL databases. Usually, the developer uses distinct code patterns for each one of these storage engines. For memory storage, primitives and simple data structures are used, like lists and hashes. And for SQL databases, the most common pattern is the repository + unit of work. While this seems the right thing to be done (since you cannot perform advanced queries on a Redis database - for instance), sometimes the developer doesn't needs the specific capabilities of that engine but he will implement the storage engine specific pattern instead of using simple data structures.

For instance, there's no semantic difference between a repository ```GetById``` method and a ```Dictionary``` (hash table) ```TryGetValue``` method. But even if the application only uses the first method, probably the developer will implement something like the repository pattern for SQL data access. But what happens if the persistence layer needs to be moved to Redis or Mongodb? Probably, the developer will implement the repository pattern for the target engine, leaving some methods that are not supported empty (like queries on Redis). Or maybe, he will need to refactor the code that uses the storage class...

The idea behind this library is expose a common layer that can be used with multiple storage engines, while isolating the specific capabilities of each one, allowing the developer compose the application storage infrastructure accordingly to its needs. It starts from common data structures, expanding according to the capabilities of each target engine. 

## When to use it?

* When you have multiple storage engines in your application
* When you do not need referential integrity (do you really need it?)
* When the data can be denormalized
* When you need good performance

## Data structures

### Primitive structures

Name             | Description												| Implementations
-----------------|----------------------------------------------------------|----------------
Map              | Mapper that provides fast access to a value using a key. | Memory, Redis, SQL
Set              | Set of unique items.										| Memory, Redis, SQL
Queue            | FIFO storage container.									| Memory, Redis
QueryableStorage | Storage that supports queries.							| Memory, SQL

### Composite structures

Name     | Description                              | Implementations
---------|------------------------------------------|----------------
SetMap   | Map that contains a set on unique items. | Memory, Redis, SQL
QueueMap | Map that contains a queue of items.      | Memory, Redis

### Extended structures

Name             | Parent | Description													| Implementations
-----------------|--------|-------------------------------------------------------------|----------------
ExpirableKeyMap  | Map    | Map that supports key expiration.							| Memory, Redis
ItemSetMap       | SetMap | SetMap that allows to get an specific item in the set.		| Memory, Redis, SQL
KeyQueryableMap  | Map    | Map that supports queries for its keys.						| Memory, SQL
KeysMap          | Map    | Map service that provides direct access to stored keys.		| Memory, SQL
NumberMap        | Map    | Map for number values with atomic increment and decrement support. | Memory, Redis
PropertyMap      | Map    | Map that allows the insertion and update of specific properties of the value document. | Memory, Redis, SQL
UpdatableMap     | Map    | Map that supports value updates under specific conditions.	| Memory, SQL

### Other

Name             | Description                                        
-----------------|------------------------------------------------------------
AsyncEnumerable  | Async implementation of ```IEnumerable<T>``` interface 

## Current supported storage engines

* Memory (System.Collections)
* SQL Server
* Redis

### NuGet

* [Takenet.Elephant](https://nuget.org/packages/Takenet.Elephant/)
* [Takenet.Elephant.Redis](https://nuget.org/packages/Takenet.Elephant.Redis/)
* [Takenet.Elephant.Sql](https://nuget.org/packages/Takenet.Elephant.Sql/)

