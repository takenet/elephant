using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
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
            : this(setName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, useScanOnEnumeration)
        {
            
        }

        public RedisSet(string setName, IConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None, bool useScanOnEnumeration = true)
            : base(setName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            if (serializer == null) throw new ArgumentNullException(nameof(serializer));
            _serializer = serializer;
            _useScanOnEnumeration = useScanOnEnumeration;
        }

        #region ISet<T> Members

        public virtual Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetAddAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetRemoveAsync(Name, _serializer.Serialize(value), WriteFlags);
        }

        public virtual async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
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

            return new AsyncEnumerableWrapper<T>(values.Select(value => _serializer.Deserialize(value)));
        }

        public virtual Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var database = GetDatabase();
            return database.SetContainsAsync(Name, _serializer.Serialize(value), ReadFlags);
        }

        public virtual Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.SetLengthAsync(Name, ReadFlags);
        }

        #endregion
    }
}
