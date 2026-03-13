using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dawn;
using Microsoft.Azure.EventHubs;

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
        private readonly ReceiverOptions _receiverOptions;
        private readonly TimeSpan _minDequeueRetryDelay;
        private readonly TimeSpan _maxDequeueRetryDelay;
        private readonly ISerializer<T> _serializer;
        private readonly EventHubClient _eventHubClient;
        private PartitionReceiver _receiver;
        private readonly SemaphoreSlim _openSemaphore;

        public AzureEventHubReceiverQueue(
            string eventHubName,
            string eventHubConnectionString,
            ISerializer<T> serializer,
            string consumerGroupName,
            string partitionId, 
            EventPosition eventPosition, 
            ReceiverOptions receiverOptions = null,
            int minDequeueRetryDelay = 250,
            int maxDequeueRetryDelay = 30000)
        {
            Guard.Argument(eventHubName).NotNull().NotEmpty();
            Guard.Argument(eventHubConnectionString).NotNull().NotEmpty();
            _consumerGroupName = Guard.Argument(consumerGroupName).NotNull().Value;
            _eventPosition = Guard.Argument(eventPosition).NotNull().Value;
            _partitionId = Guard.Argument(partitionId).NotNull().Value;
            _receiverOptions = receiverOptions;
            Guard.Argument(minDequeueRetryDelay).Max(maxDequeueRetryDelay);
            _minDequeueRetryDelay = TimeSpan.FromMilliseconds(minDequeueRetryDelay);
            _maxDequeueRetryDelay = TimeSpan.FromMilliseconds(maxDequeueRetryDelay);
            _serializer = Guard.Argument(serializer).NotNull().Value;
            
            _eventHubClient = EventHubClient.CreateFromConnectionString(
                new EventHubsConnectionStringBuilder(eventHubConnectionString)
                {
                    EntityPath = eventHubName
                }.ToString());
            _openSemaphore = new SemaphoreSlim(1);
        }
        
        public async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await OpenIfRequiredAsync(cancellationToken);
            var eventDatas = await _receiver.ReceiveAsync(1, _minDequeueRetryDelay);
            var eventData = eventDatas?.FirstOrDefault();
            return CreateItem(eventData);
        }
        
        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            var interval = new ExponentialInterval(_minDequeueRetryDelay, _maxDequeueRetryDelay);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var eventDatas = await _receiver.ReceiveAsync(1, interval.Interval);
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
            await (_receiver?.CloseAsync() ?? Task.CompletedTask);
            await _eventHubClient.CloseAsync();
        }

        private async Task<bool> OpenIfRequiredAsync(CancellationToken cancellationToken)
        {
            if (_receiver != null) return false;

            await _openSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_receiver != null) return false;
                _receiver = _eventHubClient.CreateReceiver(_consumerGroupName, _partitionId, _eventPosition, _receiverOptions);
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
            return _serializer.Deserialize(Encoding.UTF8.GetString(eventData.Body.Array));            
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