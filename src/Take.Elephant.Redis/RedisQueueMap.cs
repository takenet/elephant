using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public class RedisQueueMap<TKey, TItem> : MapBase<TKey, IBlockingQueue<TItem>>, IBlockingQueueMap<TKey, TItem>, IQueueMap<TKey, TItem>, IMap<TKey, IBlockingQueue<TItem>>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisQueueMap(string mapName, string configuration, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : this(mapName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags)
        {

        }

        public RedisQueueMap(string mapName, IConnectionMultiplexer connectionMultiplexer, ISerializer<TItem> serializer, int db = 0, CommandFlags readFlags = CommandFlags.None, CommandFlags writeFlags = CommandFlags.None)
            : base(mapName, connectionMultiplexer, db, readFlags, writeFlags)
        {
            _serializer = serializer;
        }

        #region IMap<TKey,IBlockingQueue<TItem>> Members

        public override Task<bool> TryAddAsync(TKey key,
            IBlockingQueue<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default) => TryAddAsync(key, (IQueue<TItem>) value, overwrite);

        public virtual async Task<bool> TryAddAsync(TKey key,
            IQueue<TItem> value,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (value == null) throw new ArgumentNullException(nameof(value));

            var internalQueue = value as InternalQueue;
            if (internalQueue != null) return internalQueue.Key.Equals(key) && overwrite;

            var database = GetDatabase() as IDatabase;
            if (database == null) throw new NotSupportedException("The database instance type is not supported");

            var redisKey = GetRedisKey(key);
            if (await database.KeyExistsAsync(redisKey) && !overwrite) return false;

            var transaction = database.CreateTransaction();
            var commandTasks = new List<Task>();
            if (overwrite) commandTasks.Add(transaction.KeyDeleteAsync(redisKey));

            internalQueue = CreateQueue(key, transaction);

            var queue = await CloneAsync(value).ConfigureAwait(false);
            while (await queue.GetLengthAsync().ConfigureAwait(false) > 0)
            {
                var item = await queue.DequeueOrDefaultAsync().ConfigureAwait(false);
                commandTasks.Add(internalQueue.EnqueueAsync(item));
            }

            var success = await transaction.ExecuteAsync(WriteFlags).ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        async Task<IQueue<TItem>> IMap<TKey, IQueue<TItem>>.GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default) => await GetValueOrDefaultAsync(key).ConfigureAwait(false);

        public override async Task<IBlockingQueue<TItem>> GetValueOrDefaultAsync(TKey key,
            CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key)).ConfigureAwait(false))
            {
                return CreateQueue(key);
            }

            return null;
        }

        public Task<IQueue<TItem>> GetValueOrEmptyAsync(TKey key) 
            => CreateQueue(key).AsCompletedTask<IQueue<TItem>>();

        public override Task<bool> TryRemoveAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key), WriteFlags);
        }

        public override Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key), ReadFlags);
        }

        #endregion

        protected virtual InternalQueue CreateQueue(TKey key, ITransaction transaction = null)
        {
            return new InternalQueue(key, GetRedisKey(key), _serializer, ConnectionMultiplexer, Db, ReadFlags, WriteFlags, transaction);
        }

        private static async Task<IQueue<TItem>> CloneAsync(IQueue<TItem> queue)
        {
            var cloneable = queue as ICloneable;
            if (cloneable != null) return (IQueue<TItem>) cloneable.Clone();
            
            var clone = new Memory.Queue<TItem>();
            await queue.CopyToAsync(clone).ConfigureAwait(false);
            return clone;
        }

        protected class InternalQueue : RedisQueue<TItem>
        {            
            private readonly ITransaction _transaction;

            public InternalQueue(TKey key, string queueName, ISerializer<TItem> serializer, IConnectionMultiplexer connectionMultiplexer, int db, CommandFlags readFlags, CommandFlags writeFlags, ITransaction transaction = null)
                : base(queueName, connectionMultiplexer, serializer, db, writeFlags)
            {
                _transaction = transaction;
                if (key == null) throw new ArgumentNullException(nameof(key));
                Key = key;
            }

            public TKey Key { get; }

            protected override IDatabaseAsync GetDatabase()
            {
                return _transaction ?? base.GetDatabase();
            }
        }
    }
}
