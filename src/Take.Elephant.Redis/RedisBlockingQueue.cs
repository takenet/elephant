using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using Take.Elephant.Adapters;
using Take.Elephant.Redis.Serializers;

namespace Take.Elephant.Redis
{
    public class RedisBlockingQueue<T> : IBlockingQueue<T>, IDisposable
    {
        private readonly RedisQueue<T> _redisQueue;
        private readonly RedisBus<string,string> _redisBus;
        private readonly BusBlockingQueueAdapter<T> _busBlockingQueueAdapter;
        
        public RedisBlockingQueue(
            string queueName,
            string configuration,
            ISerializer<T> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
        {
            _redisQueue = new RedisQueue<T>(
                queueName,
                configuration,
                serializer,
                db,
                readFlags,
                writeFlags);
            _redisBus = new RedisBus<string, string>(
                queueName,
                configuration,
                new StringSerializer(),
                TimeSpan.FromSeconds(60),
                db,
                readFlags,
                writeFlags);
            _busBlockingQueueAdapter = new BusBlockingQueueAdapter<T>(_redisQueue, _redisBus, queueName);
        }

        public RedisBlockingQueue(
            string queueName,
            IConnectionMultiplexer connectionMultiplexer,
            ISerializer<T> serializer,
            int db = 0,
            CommandFlags readFlags = CommandFlags.None,
            CommandFlags writeFlags = CommandFlags.None)
        {
            _redisQueue = new RedisQueue<T>(
                queueName,
                connectionMultiplexer,
                serializer,
                db,
                readFlags,
                writeFlags);
            _redisBus = new RedisBus<string, string>(
                queueName,
                connectionMultiplexer,
                new StringSerializer(),
                TimeSpan.FromSeconds(60),
                db,
                readFlags,
                writeFlags);
            _busBlockingQueueAdapter = new BusBlockingQueueAdapter<T>(_redisQueue, _redisBus, queueName);            
        }

        public virtual Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default) 
            => _busBlockingQueueAdapter.DequeueOrDefaultAsync(cancellationToken);

        public virtual Task<T> DequeueAsync(CancellationToken cancellationToken) 
            => _busBlockingQueueAdapter.DequeueAsync(cancellationToken);

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default) 
            => _busBlockingQueueAdapter.EnqueueAsync(item, cancellationToken);

        public virtual Task<long> GetLengthAsync(CancellationToken cancellationToken = default) 
            => _busBlockingQueueAdapter.GetLengthAsync(cancellationToken);
        
        public void Dispose()
        {
            _redisQueue.Dispose();
            _redisBus.Dispose();
            _busBlockingQueueAdapter.Dispose();
        }
    }
}