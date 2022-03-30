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
        private readonly TimeSpan? _emptyIndicatorExpiration;

        public RedisSet(string setName,
                        string configuration,
                        ISerializer<T> serializer,
                        int db = 0,
                        CommandFlags readFlags = CommandFlags.None,
                        CommandFlags writeFlags = CommandFlags.None,
                        bool useScanOnEnumeration = true,
                        bool supportEmptySets = false,
                        TimeSpan? emptyIndicatorExpiration = default)
            : this(setName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, useScanOnEnumeration, true, supportEmptySets, emptyIndicatorExpiration)
        {
        }

        public RedisSet(string setName,
                        IConnectionMultiplexer connectionMultiplexer,
                        ISerializer<T> serializer,
                        int db = 0,
                        CommandFlags readFlags = CommandFlags.None,
                        CommandFlags writeFlags = CommandFlags.None,
                        bool useScanOnEnumeration = true,
                        bool disposeMultiplexer = false,
                        bool supportEmptySets = false,
                        TimeSpan? emptyIndicatorExpiration = default)
            : base(setName, connectionMultiplexer, db, readFlags, writeFlags, disposeMultiplexer)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _useScanOnEnumeration = useScanOnEnumeration;
            _supportEmptySets = supportEmptySets;
            _emptyIndicatorExpiration = emptyIndicatorExpiration;
        }

        public virtual async Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var database = GetDatabase();
            // This must be done this way (instead of awaiting each one separately)
            // otherwise, a "deadlock" would occur when database is an instance of ITransaction
            // and the caller awaits this method.
            // See https://stackoverflow.com/questions/25976231/stackexchange-redis-transaction-methods-freezes
            // GetDatabase() may return an ITransaction (which is-a IDatabase). For an example, see the overriden impl
            // of InternalSet.GetDatabase
            var tasks = new List<Task> { database.SetAddAsync(Name, _serializer.Serialize(value), WriteFlags) };

            if (_supportEmptySets)
            {
                tasks.Add(database.StringSetAsync($"{GetEmptySetIndicatorForKey(Name)}", false.ToString(), _emptyIndicatorExpiration, flags: WriteFlags));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
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
            return database.SetRemoveAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual async IAsyncEnumerable<T> AsEnumerableAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (!(GetDatabase() is IDatabase database))
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

        public virtual Task<bool> ContainsAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            var database = GetDatabase();
            return database.SetContainsAsync(Name, _serializer.Serialize(value), ReadFlags);
        }

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.SetLengthAsync(Name, ReadFlags);
        }

        internal static RedisKey GetEmptySetIndicatorForKey(string key) => $"{{{key}}}{EMPTY_SET_INDICATOR}";

    }
}
