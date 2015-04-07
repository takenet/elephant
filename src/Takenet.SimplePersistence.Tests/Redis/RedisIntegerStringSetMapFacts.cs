using System;

using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Memory;
using Takenet.SimplePersistence.Redis;
using Takenet.SimplePersistence.Redis.Serializers;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Redis
{
    [Collection("Redis")]
    public class RedisIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string MapName = "integer-strings";

        public RedisIntegerStringSetMapFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IMap<int, ISet<string>> Create()
        {            
            _redisFixture.Server.FlushDatabase();            
            var setMap = new RedisSetMap<int, string>(MapName, _redisFixture.Connection.Configuration, new StringSerializer());
            return setMap;
        }

        public override ISet<string> CreateValue(int key)
        {
            var set = new HashSet<string>();
            set.AddAsync(Fixture.Create<string>()).Wait();
            return set;
        }

        [Fact(DisplayName = "AddExistingKeyAndValueFails")]
        public async override Task AddExistingKeyAndValueFails()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, false);

            // Assert
            Check.That(actual).IsFalse();
            Check.That(await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "AddNewKeyAndValueSucceeds")]
        public async override Task AddNewKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            Check.That(actual).IsTrue();
            Check.That(await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "GetExistingKeyReturnsValue")]
        public async override Task GetExistingKeyReturnsValue()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);

            // Act
            var actual = await map.GetValueOrDefaultAsync(key);

            // Assert
            Check.That(await actual.AsEnumerableAsync()).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "OverwriteExistingKeyAndValueSucceeds")]
        public async override Task OverwriteExistingKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, true);

            // Assert            
            Check.That(actual).IsTrue();
            Check.That(await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ContainsExactly(await newValue.AsEnumerableAsync());
        }
    }
}
