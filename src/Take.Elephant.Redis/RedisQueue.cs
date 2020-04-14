using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Take.Elephant.Redis
{
    public class RedisQueue<T> : StorageBase<T>, IQueue<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly SemaphoreSlim _semaphore;
        
        public RedisQueue(
            string queueName,
            string configuration,
            ISerializer<T> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
            : this(queueName, StackExchange.Redis.ConnectionMultiplexer.Connect(configuration), serializer, db, readFlags, writeFlags, true)
        {
        }

        public RedisQueue(
            string queueName,
            IConnectionMultiplexer connectionMultiplexer,
            ISerializer<T> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None,
            bool disposeMultiplexer = false)
            : base(queueName, connectionMultiplexer, db, readFlags, writeFlags, disposeMultiplexer)
        {
            _serializer = serializer;
            _semaphore = new SemaphoreSlim(1);
        }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            var database = GetDatabase();
            return database.ListLeftPushAsync(Name, _serializer.Serialize(item), flags: WriteFlags);
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            var result = await database.ListRightPopAsync(Name, ReadFlags).ConfigureAwait(false);
            return !result.IsNull ? _serializer.Deserialize((string)result) : default(T);
        }

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            var database = GetDatabase();
            return database.ListLengthAsync(Name, ReadFlags);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _semaphore.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}