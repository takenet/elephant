using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Takenet.Elephant.RabbitMQ
{
    public class RabbitMQQueue<T> : IBlockingQueue<T>
    {
        private readonly ISerializer<T> _serializer;

        public RabbitMQQueue(string queueName, IConnection rabbitMQConnection, ISerializer<T> serializer)
        {
            _serializer = serializer;
        }

        #region IQueue<T> Members

        public async Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            //var database = GetDatabase();
            //var shouldCommit = false;


            //var enqueueTask = transaction.ListLeftPushAsync(_name, _serializer.Serialize(item));
            //var publishTask = transaction.PublishAsync(_channelName, string.Empty, CommandFlags.FireAndForget);

            //if (shouldCommit &&
            //    !await transaction.ExecuteAsync().ConfigureAwait(false))
            //{
            //    throw new Exception("The transaction has failed");                    
            //}

            //await Task.WhenAll(enqueueTask, publishTask).ConfigureAwait(false);
            throw new NotImplementedException();
        }

        public async Task<T> DequeueOrDefaultAsync()
        {
            //var database = GetDatabase();
            //var result = await database.ListRightPopAsync(_name).ConfigureAwait(false);
            //return !result.IsNull ? _serializer.Deserialize((string)result) : default(T);
            throw new NotImplementedException();
        }

        public Task<long> GetLengthAsync()
        {
            //var database = GetDatabase();
            //return database.ListLengthAsync(_name);
            throw new NotImplementedException();
        }

        #endregion

        #region IBlockingQueue<T> Members

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            //var tcs = new TaskCompletionSource<T>();
            //var registration = cancellationToken.Register(() => tcs.TrySetCanceled());
            //_promisesQueue.Enqueue(Tuple.Create(tcs, registration));
            //GetSubscriber().Publish(_channelName, string.Empty, CommandFlags.FireAndForget);
            //return tcs.Task;
            throw new NotImplementedException();
        }

        #endregion
    }
}
