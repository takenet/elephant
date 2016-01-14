using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Takenet.Elephant.RabbitMQ
{
    public class RabbitMQQueue<T> : StorageBase<T>, IBlockingQueue<T>
    {
        private readonly ISerializer<T> _serializer;

        public RabbitMQQueue(string queueName, IConnection rabbitMQConnection, ISerializer<T> serializer)
            : base(queueName, rabbitMQConnection)
        {
            _serializer = serializer;
        }

        #region IQueue<T> Members

        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            
            GetModel().BasicPublish(exchange: "",
                               routingKey: _queueName,
                               basicProperties: null,
                               body: Encoding.UTF8.GetBytes(_serializer.Serialize(item)));

            return Task.FromResult(0);
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            BasicGetResult result = GetModel().BasicGet(GetQueueName(), false);
            if (result == null)
            {
                return Task.FromResult<T>(default(T));
            }

            GetModel().BasicAck(result.DeliveryTag, false);
            return Task.FromResult<T>(_serializer.Deserialize(Encoding.UTF8.GetString(result.Body)));
        }

        public Task<long> GetLengthAsync()
        {
            return Task.FromResult<long>(GetMessageCount());
        }

        #endregion

        #region IBlockingQueue<T> Members

        public Task<T> DequeueAsync(CancellationToken cancellationToken)
        {

            throw new NotImplementedException();
        }

        #endregion
    }
}
