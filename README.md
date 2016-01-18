# Elephant

Persistence library that provides common data structures as composable elements to abstract any storage engine, including SQL databases.

![TC](https://take-teamcity1.azurewebsites.net/app/rest/builds/buildType:(id:Elephant_Master)/statusIcon)

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

## Requirements (to build from source)
* C# 6 (Visual Studio 2015 or better)

### NuGet

* [Takenet.Elephant](https://nuget.org/packages/Takenet.Elephant/)
* [Takenet.Elephant.Redis](https://nuget.org/packages/Takenet.Elephant.Redis/)
* [Takenet.Elephant.Sql](https://nuget.org/packages/Takenet.Elephant.Sql/)

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
IAsyncEnumerable<Data> enumerable = await set.AsEnumerableAsync();

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

Please check the project ```Takenet.Elephant.Samples``` for more samples and the ```Takenet.Elephant.Tests``` project for details of each supported structure.
