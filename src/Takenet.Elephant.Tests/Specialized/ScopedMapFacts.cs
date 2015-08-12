using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Specialized.Scoping;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests.Specialized
{
    public abstract class ScopedMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public override IMap<TKey, TValue> Create()
        {
            return Create(CreateMap(), CreateMapScope(CreateScopeName(), CreateKeysSetMap()));
        }

        public ScopedMap<TKey, TValue> Create(IMap<TKey, TValue> map, MapScope scope)
        {
            return new ScopedMap<TKey, TValue>(
                map,
                scope,
                CreateKeySerializer());
        }

        public virtual string CreateScopeName()
        {
            return Fixture.Create<string>();
        }

        public virtual MapScope CreateMapScope(string scopeName, ISetMap<string, string> keysSetMap)
        {
            return new MapScope(scopeName, keysSetMap);
        }                

        public abstract IMap<TKey, TValue> CreateMap();

        public abstract ISetMap<string, string> CreateKeysSetMap();

        public abstract ISerializer<TKey> CreateKeySerializer();


        [Fact(DisplayName = "ClearScopeAfterAddingItemsShouldClearMap")]
        public virtual async Task ClearScopeAfterAddingItemsShouldClearMap()
        {
            // Arrange
            var map = CreateMap();
            var scopeName = CreateScopeName();
            var keysSetMap = CreateKeysSetMap();
            var scope = CreateMapScope(scopeName, keysSetMap);
            var scopedMap = Create(map, scope);
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
            await scope.ClearAsync();

            // Assert
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key1));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key2));
            AssertIsFalse(await scopedMap.ContainsKeyAsync(key3));            
        }

    }
}
