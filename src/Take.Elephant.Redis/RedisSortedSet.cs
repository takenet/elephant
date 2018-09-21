using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Take.Elephant.Redis
{
    public class RedisSortedSet<T> : StorageBase<T>, ISortedSet<T>
    {
        private readonly ISerializer<T> _serializer;

        protected RedisSortedSet(string name, ISerializer<T> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags)
            : base(name, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public RedisSortedSet(string name, string configuration, int db, ISerializer<T> serializer, CommandFlags readFlags, CommandFlags writeFlags)
            : base(name, configuration, db, readFlags, writeFlags)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var database = GetDatabase();

            var values = await database.SortedSetRangeByScoreAsync(Name);
            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public async Task<T> DequeueMaxOrDefaultAsync()
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByRankAsync(Name, -2, -1);
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
            var values = await database.SortedSetRangeByRankAsync(Name, 0, 1);
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
            return database.SortedSetAddAsync(Name, _serializer.Serialize(item), score);
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.SortedSetLengthAsync(Name);
        }
    }
}