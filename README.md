# Elephant

![Logo](/logo/logo-horizontal.png)

Persistence library that provides common data structures as composable elements to abstract any storage engine, including SQL databases.

[![Build status](https://ci.appveyor.com/api/projects/status/g7bhodvyl8ymy9dp?svg=true)](https://ci.appveyor.com/project/Take/elephant)

## Introduction

Today's applications store data in different places like the own process memory, SQL databases, Redis, and other NoSQL databases. Usually, the developer uses distinct code patterns for each one of these storage engines. For memory storage, primitives and simple data structures are used, like lists and hashes. And for SQL databases, the most common pattern is the repository + unit of work. While this seems like the right thing to do (since you cannot perform advanced queries on a Redis database - for instance), sometimes the developer doesn't need the specific capabilities of that engine, and so there's no need to implement the storage engine specific pattern instead of using simple data structures.

For instance, there's no semantic difference between a repository ```GetById``` method and a ```Dictionary``` (hash table) ```TryGetValue``` method. But even if the application only uses the first method, probably the developer will implement something like the repository pattern for SQL data access. But what happens if the persistence layer needs to be moved to Redis or Elasticsearch? Probably, the developer will implement the repository pattern for the target engine, leaving empty some methods that are not supported (like queries on Redis). Or maybe, they will need to refactor the code that uses the storage class...

The idea behind this library is to expose a common layer that can be used with multiple storage engines, while isolating the specific capabilities of each one, allowing the developer to compose the application storage infrastructure accordingly to its needs. It starts from common data structures, expanding according to the capabilities of each target engine. 

## When to use it?

* When you have multiple storage engines in your application
* When you do not need referential integrity (do you really need it?)
* When the data can be denormalized
* When you need good performance

## Data structures

### Primitive structures

Name             | Description												| Implementations
-----------------|----------------------------------------------------------|----------------
Map              | Mapper that provides fast access to a value using a key. | Memory, Redis, SQL Server, PostgreSQL, Elasticsearch
Set              | Set of unique items.										| Memory, Redis, SQL Server, PostgreSQL, Elasticsearch
Queue            | FIFO storage container.									| Memory, Redis, RabbitMQ, MSMQ
QueryableStorage | Storage that supports queries.							| Memory, SQL Server, PostgreSQL

### Composite structures

Name     | Description                              | Implementations
---------|------------------------------------------|----------------
SetMap   | Map that contains a set on unique items. | Memory, Redis, SQL Server, PostgreSQL
QueueMap | Map that contains a queue of items.      | Memory, Redis

### Extended structures

Name             | Parent | Description													| Implementations
-----------------|--------|-------------------------------------------------------------|----------------
BlockingQueue    | Queue  | A queue that allows awaiting for the next item asynchronously. | Memory, Redis, RabbitMQ, Kafka, Azure Storage, Azure Event Hub
ExpirableKeyMap  | Map    | Map that supports key expiration.							| Memory, Redis
ItemSetMap       | SetMap | SetMap that allows to get an specific item in the set.		| Memory, Redis, SQL Server, PostgreSQL
KeyQueryableMap  | Map    | Map that supports queries for its keys.						| Memory, SQL Server, PostgreSQL
KeysMap          | Map    | Map service that provides direct access to stored keys.		| Memory, SQL Server, PostgreSQL
NumberMap        | Map    | Map for number values with atomic increment and decrement support. | Memory, Redis
PropertyMap      | Map    | Map that allows the insertion and update of specific properties of the value document. | Memory, Redis, SQL
UpdatableMap     | Map    | Map that supports value updates under specific conditions.	| Memory, SQL Server, PostgreSQL

## Current supported storage engines

* Memory (System.Collections)
* Redis
* SQL Server
* PostgreSQL
* Azure Storage Queues
* Azure Service Bus
* Azure Event Hub
* Kafka
* Elasticsearch

## Requirements (to build from source)
* C# 8.0
* .NET Core 3.1

### NuGet

* [Take.Elephant](https://nuget.org/packages/Take.Elephant/)
* [Take.Elephant.Redis](https://nuget.org/packages/Take.Elephant.Redis/)
* [Take.Elephant.Sql](https://nuget.org/packages/Take.Elephant.Sql/)
* [Take.Elephant.Sql.PostgreSql](https://nuget.org/packages/Take.Elephant.Sql.PostgreSql/)
* [Take.Elephant.Azure](https://nuget.org/packages/Take.Elephant.Azure/)
* [Take.Elephant.Kafka](https://nuget.org/packages/Take.Elephant.Kafka/)

### Samples


#### Map

```csharp
// Creates an in-memory map
IMap<Guid, Data> map = new Map<Guid, Data>();

var id = Guid.NewGuid();
var data = new Data() {Name = "A name", Value = 5};

// Adds without overwrite any existing value in the key
if (await map.TryAddAsync(id, data))
    Console.WriteLine($"A value for key '{id}' was set successfully");

var newData = new Data() {Name = "A new name", Value = 1};

// Adds the value, overwriting if the key is already set
if (await map.TryAddAsync(id, newData, true))
    Console.WriteLine($"A new value for the key '{id}' was set successfully");

// Gets the value (or the type default) for the key
var existingData = await map.GetValueOrDefaultAsync(id);

// Checks if the key exists
if (await map.ContainsKeyAsync(id))
    Console.WriteLine($"The key '{id}' exists in the map");

// Removes the value defined in the key
if (await map.TryRemoveAsync(id))
    Console.WriteLine($"The value for the key '{id}' was removed");

```
#### Set

```csharp
// Creates an in-memory set
ISet<Data> set = new Set<Data>();

// A set is a collection of unique items
await set.AddAsync(data);

// Adding the same item again overwrites the existing. 
// Usually this doesn't makes any difference, but depend of how the implementation compares the values.
// For instance, the memory set uses an equality comparer, and the SQL uses the item's primary key values.
await set.AddAsync(data);

if (await set.ContainsAsync(data))
    Console.WriteLine($"The value '{data}' exists in the set");

// The set also supports the IAsyncEnumerable interface, that allows async enumeration of the items.
IAsyncEnumerable<Data> enumerable = set.AsEnumerableAsync();

// Some async extensions are available
Console.WriteLine($"There are '{await enumerable.CountAsync()}' items in the set, which are:");
await enumerable.ForEachAsync(i => Console.WriteLine($"- {i}"), CancellationToken.None);

```

#### SetMap

```csharp
// Creates an in-memory set map
ISetMap<Guid, Data> setMap = new SetMap<Guid, Data>();

var id = Guid.NewGuid();
var data = new Data() { Name = "A name", Value = 5 };

// A SetMap is just a map of sets with special extensions methods

// Uses an extension method that adds an item to a set in the key
await setMap.AddItemAsync(id, data);
var set = await setMap.GetValueOrDefaultAsync(id);
if (await set.ContainsAsync(data))
    Console.WriteLine($"The value '{data}' is present in the set");

// Removes the item from the set map
if (await setMap.TryRemoveItemAsync(id, data))
    Console.WriteLine($"The item of the set in the key '{id}' was removed");

```

Please check the project ```Take.Elephant.Samples``` for more samples and the ```Take.Elephant.Tests``` project for details of each supported structure.
