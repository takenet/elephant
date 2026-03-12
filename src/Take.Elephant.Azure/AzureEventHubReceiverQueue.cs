using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Dawn;

namespace Take.Elephant.Azure
{
    /// <summary>
    /// TODO: Incomplete implementation, should remain internal. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class AzureEventHubReceiverQueue<T> : IReceiverQueue<T>, IBlockingReceiverQueue<T>, IBatchReceiverQueue<T>, IOpenable, ICloseable
    {
        private readonly string _consumerGroupName;
        private readonly EventPosition _eventPosition;
        private readonly string _partitionId;
        private readonly TimeSpan _minDequeueRetryDelay;
        private readonly TimeSpan _maxDequeueRetryDelay;
        private readonly ISerializer<T> _serializer;
        private readonly string _eventHubConnectionString;
        private readonly string _eventHubName;
        private EventHubConsumerClient _consumerClient;
        private readonly SemaphoreSlim _openSemaphore;

        public AzureEventHubReceiverQueue(
            string eventHubName,
            string eventHubConnectionString,
            ISerializer<T> serializer,
            string consumerGroupName,
            string partitionId, 
            EventPosition eventPosition, 
            int minDequeueRetryDelay = 250,
            int maxDequeueRetryDelay = 30000)
        {
            Guard.Argument(eventHubName).NotNull().NotEmpty();
            Guard.Argument(eventHubConnectionString).NotNull().NotEmpty();
            _consumerGroupName = Guard.Argument(consumerGroupName).NotNull().Value;
            _eventPosition = eventPosition;
            _partitionId = Guard.Argument(partitionId).NotNull().Value;
            Guard.Argument(minDequeueRetryDelay).Max(maxDequeueRetryDelay);
            _minDequeueRetryDelay = TimeSpan.FromMilliseconds(minDequeueRetryDelay);
            _maxDequeueRetryDelay = TimeSpan.FromMilliseconds(maxDequeueRetryDelay);
            _serializer = Guard.Argument(serializer).NotNull().Value;
            _eventHubConnectionString = eventHubConnectionString;
            _eventHubName = eventHubName;
            _openSemaphore = new SemaphoreSlim(1);
        }
        
        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await OpenIfRequiredAsync(cancellationToken);
            using var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cancellationSource.CancelAfter(_minDequeueRetryDelay);
            try
            {
                await foreach (var partitionEvent in _consumerClient.ReadEventsFromPartitionAsync(
                    _partitionId, _eventPosition, cancellationSource.Token))
                {
                    return CreateItem(partitionEvent.Data);
                }
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested)
            {
                // Timeout reached, return default
            }
            return default;
        }
        
        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await OpenIfRequiredAsync(cancellationToken);
            
            await foreach (var partitionEvent in _consumerClient.ReadEventsFromPartitionAsync(
                _partitionId, _eventPosition, cancellationToken))
            {
                return CreateItem(partitionEvent.Data);
            }
            
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize, CancellationToken cancellationToken)
        {
            await OpenIfRequiredAsync(cancellationToken);

            return null;
        }
        
        public async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (!await OpenIfRequiredAsync(cancellationToken))
            {
                throw new InvalidOperationException("The receiver is already open");
            }
        }
        
        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            if (_consumerClient != null)
            {
                await _consumerClient.CloseAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task<bool> OpenIfRequiredAsync(CancellationToken cancellationToken)
        {
            if (_consumerClient != null) return false;

            await _openSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_consumerClient != null) return false;
                _consumerClient = new EventHubConsumerClient(_consumerGroupName, _eventHubConnectionString, _eventHubName);
                return true;
            }
            finally
            {
                _openSemaphore.Release();
            }
        }

        private T CreateItem(EventData eventData)
        {
            if (eventData == null) return default;
            return _serializer.Deserialize(Encoding.UTF8.GetString(eventData.EventBody.ToArray()));            
        }

        internal class ExponentialInterval
        {
            private readonly TimeSpan _initialInterval;
            private readonly TimeSpan _maxInterval;

            public ExponentialInterval(TimeSpan initialInterval, TimeSpan maxInterval)
            {
                _initialInterval = initialInterval;
                _maxInterval = maxInterval;
            }
            
            public int Count { get; private set;  }
            
            public TimeSpan Interval
            {
                get
                {
                    var timeout = TimeSpan.FromTicks(_initialInterval.Ticks * (int)Math.Pow(2, Count++));
                    if (timeout < _maxInterval) return timeout;
                    return _maxInterval;
                }
            }
        }
    }
}