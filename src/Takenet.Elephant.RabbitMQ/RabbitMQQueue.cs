using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;

namespace Takenet.Elephant.RabbitMQ
{
    public class RabbitMQQueue<T> : StorageBase<T>, IBlockingQueue<T>
    {
        private readonly ISerializer<T> _serializer;

        public RabbitMQQueue(string queueName, IConnectionFactory connectionFactory, ISerializer<T> serializer)
            : base(queueName, connectionFactory)
        {
            _serializer = serializer;
        }

        #region IQueue<T> Members

        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            var model = GetModel();

            // setup EAP to TAP
            var tcs = new TaskCompletionSource<bool>();
            var basicAck = default(EventHandler<BasicAckEventArgs>);
            var basicNack = default(EventHandler<BasicNackEventArgs>);

            basicAck = new EventHandler<BasicAckEventArgs>((sender, e) =>
            {
                model.BasicAcks -= basicAck;
                tcs.TrySetResult(true);
            });
            basicNack = new EventHandler<BasicNackEventArgs>((sender, e) =>
            {
                model.BasicNacks -= basicNack;
                tcs.TrySetCanceled();
            });

            model.BasicAcks += basicAck;
            model.BasicNacks += basicNack;

            /// 2.10. IModel should not be shared between threads <see href="https://www.rabbitmq.com/releases/rabbitmq-dotnet-client/v1.5.0/rabbitmq-dotnet-client-1.5.0-user-guide.pdf"/>
            lock (model)
            {
                model.BasicPublish(exchange: "",
                                   routingKey: _queueName,
                                   mandatory: true,
                                   basicProperties: new BasicProperties() { Persistent = true },
                                   body: Encoding.UTF8.GetBytes(_serializer.Serialize(item)));
            }

            return tcs.Task;
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            var model = GetModel();
            BasicGetResult result;

            /// 2.10. IModel should not be shared between threads <see href="https://www.rabbitmq.com/releases/rabbitmq-dotnet-client/v1.5.0/rabbitmq-dotnet-client-1.5.0-user-guide.pdf"/>
            lock (model)
            {
                result = model.BasicGet(GetQueueName(), false);
            }

            if (result == null)
            {
                return Task.FromResult<T>(default(T));
            }
            
            lock (model)
            {
                model.BasicAck(result.DeliveryTag, false);
            }

            return Task.FromResult<T>(_serializer.Deserialize(Encoding.UTF8.GetString(result.Body)));
        }

        public Task<long> GetLengthAsync()
        {
            return Task.FromResult<long>(GetMessageCount());
        }

        #endregion

        #region IBlockingQueue<T> Members

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var model = GetModel();
            BasicGetResult result;

            /// 2.10. IModel should not be shared between threads <see href="https://www.rabbitmq.com/releases/rabbitmq-dotnet-client/v1.5.0/rabbitmq-dotnet-client-1.5.0-user-guide.pdf"/>
            lock(model)
            {
                result = model.BasicGet(GetQueueName(), true);
            }

            if (result == null)
            {
                var consumer = new TaskBasicConsumer(model, cancellationToken);
                lock(model)
                {
                    model.BasicConsume(GetQueueName(), false, consumer);
                }
                var body = await consumer.GetTask();
                return _serializer.Deserialize(body);
            }

            return _serializer.Deserialize(Encoding.UTF8.GetString(result.Body));
        }

        #endregion
    }
}
