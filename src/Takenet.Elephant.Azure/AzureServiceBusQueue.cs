using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.ServiceBus;

namespace Takenet.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, ICloseable
    {        
        private readonly QueueClient _queueClient;
        private readonly ISerializer<T> _serializer;
        private readonly BufferBlock<T> _receivedBuffer;

        public AzureServiceBusQueue(
            string connectionString, 
            string queueName, 
            ISerializer<T> serializer, 
            int boundedCapacity = -1, 
            Func<ExceptionReceivedEventArgs, Task> exceptionReceivedHandler = null)
        {            
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _queueClient = new QueueClient(connectionString, queueName, ReceiveMode.ReceiveAndDelete);
            _queueClient.RegisterMessageHandler(ReceiveAsync, exceptionReceivedHandler ?? (e => Task.CompletedTask));
            _receivedBuffer = new BufferBlock<T>(
                new DataflowBlockOptions()
                {
                    BoundedCapacity = boundedCapacity
                });
        }

        public Task EnqueueAsync(T item)
        {
            var serializedItem = _serializer.Serialize(item);
            return _queueClient.SendAsync(new Message(Encoding.UTF8.GetBytes(serializedItem)));
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            _receivedBuffer.TryReceive(out T item);
            return Task.FromResult(item);
        }

        public Task<long> GetLengthAsync()
        {
            

            throw new NotSupportedException();
        }

        public Task<T> DequeueAsync(CancellationToken cancellationToken) =>
            _receivedBuffer.ReceiveAsync(cancellationToken);


        private Task ReceiveAsync(Message message, CancellationToken cancellationToken)
        {
            var item = _serializer.Deserialize(Encoding.UTF8.GetString(message.Body));
            return _receivedBuffer.SendAsync(item, cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken) => _queueClient.CloseAsync();
    }
}
