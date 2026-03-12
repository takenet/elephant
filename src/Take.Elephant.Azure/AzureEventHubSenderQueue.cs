using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Dawn;

namespace Take.Elephant.Azure
{
    public class AzureEventHubSenderQueue<T> : ISenderQueue<T>, ICloseable
    {
        private readonly ISerializer<T> _serializer;
        private readonly EventHubProducerClient _producerClient;
        
        public AzureEventHubSenderQueue(string eventHubName, string eventHubConnectionString, ISerializer<T> serializer)
        {
            Guard.Argument(eventHubName).NotNull().NotEmpty();
            Guard.Argument(eventHubConnectionString).NotNull().NotEmpty();
            _serializer = Guard.Argument(serializer).NotNull().Value;

            _producerClient = new EventHubProducerClient(eventHubConnectionString, eventHubName);
        }

        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            var serializedItem = _serializer.Serialize(item);
            var eventData = new EventData(Encoding.UTF8.GetBytes(serializedItem));
            await _producerClient.SendAsync(new[] { eventData }, cancellationToken).ConfigureAwait(false);
        }

        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            await _producerClient.CloseAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}