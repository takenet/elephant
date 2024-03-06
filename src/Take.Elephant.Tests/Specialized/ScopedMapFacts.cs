using System;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Specialized.Scoping;
using AutoFixture;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class ScopedMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public override IMap<TKey, TValue> Create()
        {
            return Create(CreateMap(), CreateMapScope(CreateScopeName(), CreateKeysSetMap()), CreateIdentifier());
        }

        public virtual ScopedMap<TKey, TValue> Create(IMap<TKey, TValue> map, MapScope scope, string identifier)
        {
            return new ScopedMap<TKey, TValue>(
                map,
                scope,
                identifier,
                CreateKeySerializer());
        }

        public virtual string CreateScopeName()
        {
            return Fixture.Create<string>();
        }

        public virtual string CreateIdentifier()
        {
            return Fixture.Create<string>();
        }

        public virtual MapScope CreateMapScope(string scopeName, ISetMap<string, IdentifierKey> keysSetMap)
        {
            return new MapScope(scopeName, keysSetMap);
        }                

        public abstract IMap<TKey, TValue> CreateMap();

        public abstract ISetMap<string, IdentifierKey> CreateKeysSetMap();

        public abstract ISerializer<TKey> CreateKeySerializer();

        [Fact(DisplayName = nameof(CreateScopedMapAndAddKeyShouldAddToKeySetMap))]
        public virtual async Task CreateScopedMapAndAddKeyShouldAddToKeySetMap()
        {
            // Arrange
            var map = CreateMap();
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var identifier = CreateIdentifier();
            var scopedMap = Create(map, scope, identifier);
            var keySerializer = CreateKeySerializer();
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);

            // Act
            if (!await scopedMap.TryAddAsync(key1, value1, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key2, value2, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key3, value3, false)) throw new Exception("Could not setup the test scenario");

            // Assert
            AssertIsTrue(await keysSetMap.ContainsKeyAsync(scopeName));            
            AssertIsTrue(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = keySerializer.Serialize(key1) }));
            AssertIsTrue(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = keySerializer.Serialize(key2) }));
            AssertIsTrue(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = keySerializer.Serialize(key3) }));
        }

        [Fact(DisplayName = "ClearScopeAfterAddingItemsShouldClearMap")]
        public virtual async Task ClearScopeAfterAddingItemsShouldClearMap()
        {
            // Arrange
            var map = CreateMap();
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var identifier = CreateIdentifier();
            var scopedMap = Create(map, scope, identifier);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);
            if (!await scopedMap.TryAddAsync(key1, value1, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key2, value2, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key3, value3, false)) throw new Exception("Could not setup the test scenario");

            // Act
            await scope.ClearAsync(CancellationToken.None);

            // Assert
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key1));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key2));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key3));            
        }

        [Fact(DisplayName = "CreateAndClearAScopeWithSameNameShouldClearTheExistingOne")]
        public virtual async Task CreateAndClearAScopeWithSameNameShouldClearTheExistingOne()
        {
            // Arrange
            var map = CreateMap();
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var identifier = CreateIdentifier();
            var scopedMap = Create(map, scope, identifier);
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);
            if (!await scopedMap.TryAddAsync(key1, value1, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key2, value2, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key3, value3, false)) throw new Exception("Could not setup the test scenario");
            var newScope = CreateMapScope(scopeName, keysSetMap);
            var newScopedMap = Create(map, newScope, identifier);

            // Act
            await newScope.ClearAsync(CancellationToken.None);

            // Assert
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key1));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key2));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key3));
        }

        [Fact(DisplayName = nameof(AddMultipleMapsToAScopeShouldSucceed))]
        public virtual async Task AddMultipleMapsToAScopeShouldSucceed()
        {
            // Arrange
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var map1 = CreateMap();
            var map2 = CreateMap();
            var map3 = CreateMap();
            var identifier1 = CreateIdentifier();
            var identifier2 = CreateIdentifier();
            var identifier3 = CreateIdentifier();
            var scopedMap1 = Create(map1, scope, identifier1);
            var scopedMap2 = Create(map2, scope, identifier2);
            var scopedMap3 = Create(map3, scope, identifier3);
            var key11 = CreateKey();
            var value11 = CreateValue(key11);
            var key12 = CreateKey();
            var value12 = CreateValue(key12);
            var key13 = CreateKey();
            var value13 = CreateValue(key13);
            var key21 = CreateKey();
            var value21 = CreateValue(key21);
            var key22 = CreateKey();
            var value22 = CreateValue(key22);
            var key23 = CreateKey();
            var value23 = CreateValue(key23);
            var key31 = CreateKey();
            var value31 = CreateValue(key31);
            var key32 = CreateKey();
            var value32 = CreateValue(key32);
            var key33 = CreateKey();
            var value33 = CreateValue(key33);
            if (!await scopedMap1.TryAddAsync(key11, value11, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap1.TryAddAsync(key12, value12, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap1.TryAddAsync(key13, value13, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap2.TryAddAsync(key21, value21, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap2.TryAddAsync(key22, value22, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap2.TryAddAsync(key23, value23, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap3.TryAddAsync(key31, value31, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap3.TryAddAsync(key32, value32, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap3.TryAddAsync(key33, value33, false)) throw new Exception("Could not setup the test scenario");

            // Act
            await scope.ClearAsync(CancellationToken.None);

            // Assert
            AssertIsFalse(await scopedMap1.ContainsKeyAsync(key11));
            AssertIsFalse(await scopedMap1.ContainsKeyAsync(key12));
            AssertIsFalse(await scopedMap1.ContainsKeyAsync(key13));
            AssertIsFalse(await scopedMap2.ContainsKeyAsync(key21));
            AssertIsFalse(await scopedMap2.ContainsKeyAsync(key22));
            AssertIsFalse(await scopedMap2.ContainsKeyAsync(key23));
            AssertIsFalse(await scopedMap3.ContainsKeyAsync(key31));
            AssertIsFalse(await scopedMap3.ContainsKeyAsync(key32));
            AssertIsFalse(await scopedMap3.ContainsKeyAsync(key33));
        }

        [Fact(DisplayName = nameof(RemoveKeyFromMapShouldRemoveFromScope))]
        public virtual async Task RemoveKeyFromMapShouldRemoveFromScope()
        {
            // Arrange
            var map = CreateMap();
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var identifier = CreateIdentifier();
            var scopedMap = Create(map, scope, identifier);
            var serializer = CreateKeySerializer();
            var key1 = CreateKey();
            var value1 = CreateValue(key1);
            var key2 = CreateKey();
            var value2 = CreateValue(key2);
            var key3 = CreateKey();
            var value3 = CreateValue(key3);
            if (!await scopedMap.TryAddAsync(key1, value1, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key2, value2, false)) throw new Exception("Could not setup the test scenario");
            if (!await scopedMap.TryAddAsync(key3, value3, false)) throw new Exception("Could not setup the test scenario");

            // Act
            await scopedMap.TryRemoveAsync(key1);
            await scopedMap.TryRemoveAsync(key3);            

            // Assert
            AssertIsTrue(await keysSetMap.ContainsKeyAsync(scopeName));            
            AssertIsFalse(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = serializer.Serialize(key1)}));
            AssertIsTrue(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = serializer.Serialize(key2) }));
            AssertIsFalse(await keysSetMap.ContainsItemAsync(scopeName, new IdentifierKey() { Identifier = identifier, Key = serializer.Serialize(key3) }));            
        }
    }
}
