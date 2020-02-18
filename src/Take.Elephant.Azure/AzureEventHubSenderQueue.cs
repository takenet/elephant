using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Take.Elephant.Azure
{
    public class AzureEventHubSenderQueue<T> : ISenderQueue<T>, IBatchSenderQueue<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly EventHubProducerClient _producer;

        public AzureEventHubSenderQueue(string topic, string connectionString, ISerializer<T> serializer)
        {
            _serializer = serializer;
            _producer = new EventHubProducerClient(connectionString, topic);
        }

        public async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            using var eventBatch = await _producer.CreateBatchAsync();
            eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(_serializer.Serialize(item))));
            await _producer.SendAsync(eventBatch);
        }

        public async Task EnqueueBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            using var eventBatch = await _producer.CreateBatchAsync();
            foreach (var item in items)
            {
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(_serializer.Serialize(item))));
            }
            await _producer.SendAsync(eventBatch);
        }
    }
}