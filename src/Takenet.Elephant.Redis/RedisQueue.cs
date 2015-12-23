using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Takenet.Elephant.Redis
{
    public class RedisQueue<T> : StorageBase<T>, IBlockingQueue<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>> _promisesQueue = new ConcurrentQueue<Tuple<TaskCompletionSource<T>, CancellationTokenRegistration>>();
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
        private readonly string _channelName;

        private ISubscriber _subscriber;

        public RedisQueue(string queueName, string configuration, ISerializer<T> serializer, int db = 0)
            : this(queueName, ConnectionMultiplexer.Connect(configuration), serializer, db)
        {
            
        }

        public RedisQueue(string queueName, ConnectionMultiplexer connectionMultiplexer, ISerializer<T> serializer, int db)
            : base(queueName, connectionMultiplexer, db)
        {
            _channelName = $"{db}:{queueName}";
            _serializer = serializer;
            SubscribeChannel();
        }

        #region IQueue<T> Members

        public async Task EnqueueAsync(T item)
        {            
            if (item == null) throw new ArgumentNullException(nameof(item));
            var database = GetDatabase();
            var shouldCommit = false;

            ITransaction transaction;
            if (database is ITransaction)
            {
                transaction = (ITransaction)database;                
            }
            else if (database is IDatabase)
            {
                transaction = ((IDatabase) database).CreateTransaction();
                shouldCommit = true;
            }
            else
            {
                throw new NotSupportedException("The database instance type is not supported");
            }

            var enqueueTask = transaction.ListLeftPushAsync(_name, _serializer.Serialize(item));
            var publishTask = transaction.PublishAsync(_channelName, string.Empty, CommandFlags.FireAndForget);

            if (shouldCommit &&
                !await transaction.ExecuteAsync().ConfigureAwait(false))
            {
                throw new Exception("The transaction has failed");                    
            }
                    
            await Task.WhenAll(enqueueTask, publishTask).ConfigureAwait(false);
        }

        public async Task<T> DequeueOrDefaultAsync()
        {
            var database = GetDatabase();
            var result = await database.ListRightPopAsync(_name).ConfigureAwait(false);
            return !result.IsNull ? _serializer.Deserialize((string)result) : default(T);
        }

        public Task<long> GetLengthAsync()
        {
            var database = GetDatabase();
            return database.ListLengthAsync(_name);
        }

        #endregion

        #region IBlockingQueue<T> Members

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<T>();
            var registration = cancellationToken.Register(() => tcs.TrySetCanceled());
            _promisesQueue.Enqueue(Tuple.Create(tcs, registration));
            GetSubscriber().Publish(_channelName, string.Empty, CommandFlags.FireAndForget);
            return tcs.Task;
        }

        #endregion

        private void SubscribeChannel()
        {
            _subscriber = GetSubscriber();
            _subscriber.Subscribe(
                _channelName,
                async (c, v) =>
                {
                    Tuple<TaskCompletionSource<T>, CancellationTokenRegistration> promise = null;
                    await _semaphore.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        if (_promisesQueue.TryDequeue(out promise) && !promise.Item1.Task.IsCanceled)
                        {
                            var database = GetDatabase();
                            var result = await database.ListRightPopAsync(_name).ConfigureAwait(false);
                            if (result.IsNull)
                            {
                                _promisesQueue.Enqueue(promise);
                            }
                            else
                            {
                                var item = _serializer.Deserialize((string) result);
                                if (promise.Item1.TrySetResult(item))
                                {
                                    promise.Item2.Dispose();
                                }
                                else
                                {
                                    await EnqueueAsync(item).ConfigureAwait(false);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError(ex.ToString());
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                });
        }        
    }
}
