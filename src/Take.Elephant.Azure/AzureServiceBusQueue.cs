using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace Take.Elephant.Azure
{
    public class AzureServiceBusQueue<T> : IBlockingQueue<T>, ICloseable
    {
        private readonly ISerializer<T> _serializer;        
        private readonly MessageSender _messageSender;
        private readonly MessageReceiver _messageReceiver;
        private readonly BufferBlock<Message> _messageBufferBlock;

        public AzureServiceBusQueue(
            string connectionString, 
            string entityPath, 
            ISerializer<T> serializer,
            int receiverBoundedCapacity = 1)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));                        
            _messageSender = new MessageSender(connectionString, entityPath);
            _messageReceiver = new MessageReceiver(connectionString, entityPath);            
            _messageBufferBlock = new BufferBlock<Message>(
                new DataflowBlockOptions()
                {
                    BoundedCapacity = receiverBoundedCapacity,
                    EnsureOrdered = true
                });           
            _messageReceiver.RegisterMessageHandler(
                _messageBufferBlock.SendAsync,
                args => Task.CompletedTask);
        }

        public Task EnqueueAsync(T item)
        {            
            var serializedItem = _serializer.Serialize(item);
            return _messageSender.SendAsync(new Message(Encoding.UTF8.GetBytes(serializedItem)));            
        }

        public Task<T> DequeueOrDefaultAsync()
        {
            if (!_messageBufferBlock.TryReceive(out var message))
            {
                return default(T).AsCompletedTask();
            }

            return CreateItemAndCompleteAsync(message);
        }
        
        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var message = await _messageBufferBlock.ReceiveAsync(cancellationToken);
            return await CreateItemAndCompleteAsync(message);
        }

        public Task<long> GetLengthAsync()
        {
            throw new NotImplementedException();          
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            return Task.WhenAll(
                _messageSender.CloseAsync(),
                _messageReceiver.CloseAsync());
        }

        private async Task<T> CreateItemAndCompleteAsync(Message message)
        {
            var serializedItem = Encoding.UTF8.GetString(message.Body);
            var item = _serializer.Deserialize(serializedItem);                    
            await _messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            return item;
        }
    }
}
