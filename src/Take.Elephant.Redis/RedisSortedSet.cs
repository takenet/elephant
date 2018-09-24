using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    public class RedisSortedSet<T> : StorageBase<T>, ISortedSet<T>
    {
        private readonly ISerializer<T> _serializer;

        protected RedisSortedSet(string name, ISerializer<T> serializer, IConnectionMultiplexer connectionMultiplexer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(name, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public RedisSortedSet(string name, string configuration, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(name, configuration, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync(CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();

            var values = await database.SortedSetRangeByScoreAsync(Name);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public async Task<IAsyncEnumerable<KeyValuePair<double, T>>> AsEnumerableWithScoreAsync(CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();

            var values = await database.SortedSetRangeByScoreWithScoresAsync(Name);
            return new AsyncEnumerableWrapper<KeyValuePair<double, T>>(
                values.Select(k => new KeyValuePair<double, T>(k.Score, _serializer.Deserialize(k.Element)))
            );
        }

        public async Task<T> DequeueMaxOrDefaultAsync()
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByRankAsync(Name, -1, -1);
            if (values == null || values.Length == 0)
            {
                return default(T);
            }
            var result = values[0];
            await database.SortedSetRemoveAsync(Name, result);
            return !result.IsNull ? _serializer.Deserialize(result) : default(T);
        }

        public async Task<T> DequeueMinOrDefaultAsync()
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByRankAsync(Name, 0, 0);
            if (values == null || values.Length == 0)
            {
                return default(T);
            }
            var result = values[0];
            await database.SortedSetRemoveAsync(Name, result);
            return !result.IsNull ? _serializer.Deserialize(result) : default(T);
        }

        public Task EnqueueAsync(T item, double score)
        {
            var database = GetDatabase();
            return database.SortedSetAddAsync(Name, _serializer.Serialize(item), score, WriteFlags);
        }

        public Task<long> GetLengthAsync(CancellationToken cancelationToken = default)
        {
            var database = GetDatabase();
            return database.SortedSetLengthAsync(Name);
        }

        public Task<bool> RemoveAsync(T value)
        {
            var database = GetDatabase();
            return database.SortedSetRemoveAsync(Name, _serializer.Serialize(value));
        }

        public async Task<IAsyncEnumerable<T>> RangeByRankAsync(long initial = 0, long end = -1)
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByRankAsync(Name, initial, end);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }
    }
}