using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    /// <summary>
    /// Implements the <see cref="ISet{T}"/> interface using Redis set data structure.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RedisSet<T> : StorageBase<string>, ISet<T>
    {
        private const string EMPTY_SET_INDICATOR = "__ELEPHANT_EMPTY_SET_INDICATOR__";
        private readonly ISerializer<T> _serializer;
        private readonly bool _useScanOnEnumeration;
        private readonly bool _supportEmptySets;

        public RedisSet(string setName, string configuration, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : this(setName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, useScanOnEnumeration, true)
        { }

        public RedisSet(string setName, IConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true, bool disposeMultiplexer = false)
            : base(setName, connectionMultiplexer, db, readFlags, writeFlags, disposeMultiplexer)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        #region ISet<T> Members

        public virtual async Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var database = GetDatabase();
            await database.SetAddAsync(Name, _serializer.Serialize(value), WriteFlags);

            if (_supportEmptySets)
            {
                await database.StringSetAsync($"{Name}{EMPTY_SET_INDICATOR}", false, TimeSpan.FromMinutes(15), flags: WriteFlags);
            }
        }

        public virtual async Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            // No need to set the empty indicator here
            // We avoid extra trips to the database (would have to GetLength in a transaction in order to know if it's empty or not)
            // And there is no consistency issue here; The worst that could
            // happen is the indicator say that the set is not empty, when it actually is
            // which would cause one extra trip to the database when fetching values in those cases

            var database = GetDatabase();
            return await database.SetRemoveAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual async IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var database = GetDatabase() as IDatabase;
            if (database == null)
            {
                throw new NotSupportedException("The database instance is not supported");
            }

            IEnumerable<RedisValue> values;
            if (_useScanOnEnumeration)
            {
                values = database.SetScan(Name);
            }
            else
            {
                values = await database.SetMembersAsync(Name).ConfigureAwait(false);
            }

            foreach (var value in values.Select(value => _serializer.Deserialize(value)))
            {
                yield return value;
            }
        }

        public virtual async Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var database = GetDatabase();
            return await database.SetContainsAsync(Name, _serializer.Serialize(value), ReadFlags);
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return await database.SetLengthAsync(Name, ReadFlags);
        }

        #endregion
    }
}
