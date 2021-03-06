﻿using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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

        public async IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();

            var values = await database.SortedSetRangeByScoreAsync(Name);

            foreach (var value in values.Select(value => _serializer.Deserialize(value)))
            {
                yield return value;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<double, T>> AsEnumerableWithScoreAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();

            var values = await database.SortedSetRangeByScoreWithScoresAsync(Name);

            foreach (var value in values)
            {
                yield return new KeyValuePair<double, T>(value.Score, _serializer.Deserialize(value.Element));
            }
        }

        public async Task<T> RemoveMaxOrDefaultAsync(CancellationToken cancellationToken = default)
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

        public async Task<T> RemoveMinOrDefaultAsync(CancellationToken cancellationToken = default)
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

        public Task AddAsync(T item, double score, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.SortedSetAddAsync(Name, _serializer.Serialize(item), score, WriteFlags);
        }

        public Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.SortedSetLengthAsync(Name);
        }

        public Task<bool> RemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.SortedSetRemoveAsync(Name, _serializer.Serialize(value));
        }

        public async IAsyncEnumerable<T> GetRangeByRankAsync(long initial = 0, long end = -1, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByRankAsync(Name, initial, end);

            foreach (var value in values)
            {
                yield return _serializer.Deserialize(value);
            }
        }

        public async IAsyncEnumerable<T> GetRangeByScoreAsync(double start = 0, double stop = 0, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            var values = await database.SortedSetRangeByScoreAsync(Name, start, stop);
            
            foreach (var value in values)
            {
                yield return _serializer.Deserialize(value);
            }
        }
    }
}