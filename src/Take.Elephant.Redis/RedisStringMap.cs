using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="IMap{TKey,TValue}"/> interface using Redis standard key/value data structure.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class RedisStringMap<TKey, TValue> : MapBase<TKey, TValue>
    {
        protected readonly ISerializer<TValue> _serializer;

        public RedisStringMap(
            string mapName,
            string configuration,
            ISerializer<TValue> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        {
        
        }

        public RedisStringMap(
            string mapName,
            IConnectionMultiplexer connectionMultiplexer,
            ISerializer<TValue> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public override async Task<bool> TryAddWithAbsoluteExpirationAsync(TKey key, TValue value,
            DateTimeOffset expiration = default,
            bool overwrite = false, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (expiration <= DateTimeOffset.Now && expiration != default)
            {
                // If expiration is a date from the past, do not add
                return false;
            }

            var database = GetDatabase();

            if (expiration == default)
            {
                return await database.StringSetAsync(
                    GetRedisKey(key),
                    _serializer.Serialize(value),
                    when: overwrite ? When.Always : When.NotExists,
                    flags: WriteFlags);
            }

            return await database.StringSetAsync(
                GetRedisKey(key),
                _serializer.Serialize(value),
                when: overwrite ? When.Always : When.NotExists,
                expiry: expiration.Subtract(DateTimeOffset.Now),
                flags: WriteFlags);
        }

        public override Task<bool> TryAddAsync(TKey key, TValue value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var database = GetDatabase();
            return database.StringSetAsync(
                GetRedisKey(key),
                _serializer.Serialize(value),
                when: overwrite ? When.Always : When.NotExists,
                flags: WriteFlags);
        }

        public override async Task<TValue> GetValueOrDefaultAsync(TKey key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var database = GetDatabase();
            var redisValue = await database.StringGetAsync(GetRedisKey(key), ReadFlags);
            return redisValue.IsNull ? default(TValue) : _serializer.Deserialize(redisValue);
        }

        public override Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        public override Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }
    }
}
