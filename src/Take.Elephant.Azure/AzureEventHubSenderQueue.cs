using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace Take.Elephant.Azure
{
    public class AzureEventHubSenderQueue<T> : ISenderQueue<T>, ICloseable
    {
        private readonly ISerializer<T> _serializer;
        private readonly EventHubClient _eventHubClient;
        
        public AzureEventHubSenderQueue(string eventHubName, string eventHubConnectionString, ISerializer<T> serializer)
        {
            if (string.IsNullOrEmpty(eventHubName)) throw new ArgumentException("Value cannot be null or empty.", nameof(eventHubName));
            if (string.IsNullOrEmpty(eventHubConnectionString)) throw new ArgumentException("Value cannot be null or empty.", nameof(eventHubConnectionString));
            _serializer = serializer;

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
            {
                EntityPath = eventHubName
            };
            _eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        }
        
        public Task EnqueueAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            var serializedItem = _serializer.Serialize(item);
            return _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(serializedItem)));                        
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            return _eventHubClient.CloseAsync();
        }
    }
}