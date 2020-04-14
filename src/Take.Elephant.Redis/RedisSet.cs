using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly ISerializer<T> _serializer;
        private readonly bool _useScanOnEnumeration;

        public RedisSet(string setName, string configuration, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : this(setName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, useScanOnEnumeration, true)
        { }

        public RedisSet(string setName, IConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true, bool disposeMultiplexer = false)
            : base(setName, connectionMultiplexer, db, readFlags, writeFlags, disposeMultiplexer)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        #region ISet<T> Members

        public virtual Task AddAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetAddAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual Task<bool> TryRemoveAsync(T value, CancellationToken cancellationToken = default)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetRemoveAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual async IAsyncEnumerable<T> AsEnumerableAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance is not supported");
            
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
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetContainsAsync(Name, _serializer.Serialize(value), ReadFlags);
        }

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.SetLengthAsync(Name, ReadFlags);
        }

        #endregion
    }
}
