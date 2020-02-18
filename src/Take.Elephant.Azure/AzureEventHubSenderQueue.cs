using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dawn;
using Microsoft.Azure.EventHubs;

namespace Take.Elephant.Azure
{
    public class AzureEventHubSenderQueue<T> : ISenderQueue<T>, ICloseable
    {
        private readonly ISerializer<T> _serializer;
        private readonly EventHubClient _eventHubClient;
        
        public AzureEventHubSenderQueue(string eventHubName, string eventHubConnectionString, ISerializer<T> serializer)
        {
            Guard.Argument(eventHubName).NotNull().NotEmpty();
            Guard.Argument(eventHubConnectionString).NotNull().NotEmpty();
            _serializer = Guard.Argument(serializer).NotNull().Value;

            _eventHubClient = EventHubClient.CreateFromConnectionString(
                new EventHubsConnectionStringBuilder(eventHubConnectionString)
                {
                    EntityPath = eventHubName
                }.ToString());
        }

        public virtual Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            var serializedItem = _serializer.Serialize(item);
            return _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(serializedItem)));                        
        }

        public virtual Task CloseAsync(CancellationToken cancellationToken)
        {
            return _eventHubClient.CloseAsync();
        }
    }
}