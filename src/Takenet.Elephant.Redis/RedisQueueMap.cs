using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisQueueMap<TKey, TItem> : MapBase<TKey, IQueue<TItem>>, IQueueMap<TKey, TItem>
    {
        private readonly ISerializer<TItem> _serializer;

        public RedisQueueMap(string mapName, string configuration, ISerializer<TItem> serializer)
            : base(mapName, configuration)
        {
            _serializer = serializer;
        }

        #region IMap<TKey,IQueue<TItem>> Members

        public override async Task<bool> TryAddAsync(TKey key, IQueue<TItem> value, bool overwrite = false)
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

            var success = await transaction.ExecuteAsync().ConfigureAwait(false);
            await Task.WhenAll(commandTasks).ConfigureAwait(false);
            return success;
        }

        public override async Task<IQueue<TItem>> GetValueOrDefaultAsync(TKey key)
        {
            var database = GetDatabase();
            if (await database.KeyExistsAsync(GetRedisKey(key)).ConfigureAwait(false))
            {
                return CreateQueue(key);
            }

            return null;
        }

        public override Task<bool> TryRemoveAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyDeleteAsync(GetRedisKey(key));
        }

        public override Task<bool> ContainsKeyAsync(TKey key)
        {
            var database = GetDatabase();
            return database.KeyExistsAsync(GetRedisKey(key));
        }

        #endregion

        protected InternalQueue CreateQueue(TKey key, ITransaction transaction = null)
        {
            return new InternalQueue(key, GetRedisKey(key), _serializer, _connectionMultiplexer, transaction);
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

            public InternalQueue(TKey key, string queueName, ISerializer<TItem> serializer, ConnectionMultiplexer connectionMultiplexer, ITransaction transaction = null)
                : base(queueName, connectionMultiplexer, serializer)
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
