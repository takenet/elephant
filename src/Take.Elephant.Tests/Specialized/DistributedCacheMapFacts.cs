using System.Threading.Tasks;
using Shouldly;
using Take.Elephant.Specialized.Cache;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public abstract class DistributedCacheMapFacts<TKey, TValue> : MapFacts<TKey, TValue>
    {
        public override IMap<TKey, TValue> Create()
        {
            var source = CreateSource();
            var synchronizationBus = CreateSynchronizationBus();
            var synchronizationChannel = CreateSynchronizationChannel();

            return Create(source, synchronizationBus, synchronizationChannel);
        }

        public DistributedCacheMap<TKey, TValue> Create(IMap<TKey, TValue> source, IBus<string, SynchronizationEvent<TKey>> synchronizationBus, string synchronizationChannel)
        {
            return new DistributedCacheMap<TKey, TValue>(source, synchronizationBus, synchronizationChannel);
        }

        public abstract IMap<TKey, TValue> CreateSource();
        
        public abstract IBus<string, SynchronizationEvent<TKey>> CreateSynchronizationBus();

        public abstract string CreateSynchronizationChannel();

        [Fact(DisplayName = nameof(AddShouldRetrieveFromSource))]
        public async Task AddShouldRetrieveFromSource()
        {
            // Arrange
            var source = CreateSource();
            var synchronizationBus = CreateSynchronizationBus();
            var synchronizationChannel = CreateSynchronizationChannel();
            var target1 = Create(source, synchronizationBus, synchronizationChannel);
            var target2 = Create(source, synchronizationBus, synchronizationChannel);
            var target3 = Create(source, synchronizationBus, synchronizationChannel);

            var key = CreateKey();
            var value1 = CreateValue(key);
            var value2 = CreateValue(key);
            var value3 = CreateValue(key);

            // Act
            await target1.TryAddAsync(key, value1, true);
            await target2.TryAddAsync(key, value2, true);
            await target3.TryAddAsync(key, value3, true);
            
            // Assert
            var actual1 = await target1.GetValueOrDefaultAsync(key);
            var actual2 = await target2.GetValueOrDefaultAsync(key);
            var actual3 = await target3.GetValueOrDefaultAsync(key);
            actual1.ShouldBe(value3);
            actual2.ShouldBe(value3);
            actual3.ShouldBe(value3);
        }
        
        [Fact(DisplayName = nameof(AddShouldRetrieveFromCache))]
        public async Task AddShouldRetrieveFromCache()
        {
            // Arrange
            var source = CreateSource();
            var synchronizationBus = CreateSynchronizationBus();
            var synchronizationChannel = CreateSynchronizationChannel();
            var target1 = Create(source, synchronizationBus, synchronizationChannel);
            var target2 = Create(source, synchronizationBus, synchronizationChannel);
            var target3 = Create(source, synchronizationBus, synchronizationChannel);

            var key = CreateKey();
            var value1 = CreateValue(key);
            var value2 = CreateValue(key);
            var value3 = CreateValue(key);

            // Act
            await target1.TryAddAsync(key, value1, true);
            await target2.TryAddAsync(key, value2, true);
            await target3.TryAddAsync(key, value3, true);
            await target1.GetValueOrDefaultAsync(key); // Force the caching of values
            await target2.GetValueOrDefaultAsync(key);
            await target3.GetValueOrDefaultAsync(key);
            
            // Assert
            await source.TryRemoveAsync(key); // remove from source to validate if the cache was synchronized
            var actual1 = await target1.GetValueOrDefaultAsync(key);
            var actual2 = await target2.GetValueOrDefaultAsync(key);
            var actual3 = await target3.GetValueOrDefaultAsync(key);
            actual1.ShouldBe(value3);
            actual2.ShouldBe(value3);
            actual3.ShouldBe(value3);
        }
        
        [Fact(DisplayName = nameof(RemoveShouldSynchronizeTheCache))]
        public async Task RemoveShouldSynchronizeTheCache()
        {
            // Arrange
            var source = CreateSource();
            var synchronizationBus = CreateSynchronizationBus();
            var synchronizationChannel = CreateSynchronizationChannel();
            var target1 = Create(source, synchronizationBus, synchronizationChannel);
            var target2 = Create(source, synchronizationBus, synchronizationChannel);
            var target3 = Create(source, synchronizationBus, synchronizationChannel);
            var key = CreateKey();
            var value1 = CreateValue(key);
            var value2 = CreateValue(key);
            var value3 = CreateValue(key);
            await target1.TryAddAsync(key, value1, true);
            await target2.TryAddAsync(key, value2, true);
            await target3.TryAddAsync(key, value3, true);
            await target1.GetValueOrDefaultAsync(key); // Force the caching of values
            await target2.GetValueOrDefaultAsync(key);
            await target3.GetValueOrDefaultAsync(key);

            // Act
            await target2.TryRemoveAsync(key);
            
            // Assert
            var actual1 = await target1.GetValueOrDefaultAsync(key);
            var actual2 = await target2.GetValueOrDefaultAsync(key);
            var actual3 = await target3.GetValueOrDefaultAsync(key);
            actual1.ShouldBeNull();
            actual2.ShouldBeNull();
            actual3.ShouldBeNull();
        }
    }
}