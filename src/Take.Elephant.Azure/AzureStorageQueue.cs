using Dawn;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;

namespace Take.Elephant.Azure
{
    public class AzureStorageQueue<T> :
        IBlockingQueue<T>,
        IBatchReceiverQueue<T>,
        IBlockingReceiverQueue<StorageTransaction<T>>,
        IBatchReceiverQueue<StorageTransaction<T>>,
        ITransactionalStorage<T>,
        IDisposable        
        where T : class        
    {
        private readonly CloudQueue _queue;
        private readonly ISerializer<T> _serializer;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private readonly SemaphoreSlim _dequeueSemaphore;
        private readonly int _minDequeueRetryDelay;
        private readonly int _maxDequeueRetryDelay;

        private bool _queueExists;

        public AzureStorageQueue(
            string storageConnectionString,
            string queueName,
            ISerializer<T> serializer,
            bool encodeMessage = true,
            int minDequeueRetryDelay = 250,
            int maxDequeueRetryDelay = 30000)
        {
            Guard.Argument(storageConnectionString)
                .NotNull()
                .NotEmpty();
            Guard.Argument(queueName)
                .NotNull()
                .NotEmpty();
            Guard.Argument(serializer)
                .NotNull();
            Guard.Argument(minDequeueRetryDelay)
                .Positive();
            Guard.Argument(maxDequeueRetryDelay)
                .Min(minDequeueRetryDelay);
            Guard.Argument(minDequeueRetryDelay)
                .Max(maxDequeueRetryDelay);
            
            _serializer = serializer;
            _minDequeueRetryDelay = minDequeueRetryDelay;
            _maxDequeueRetryDelay = maxDequeueRetryDelay;
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var client = storageAccount.CreateCloudQueueClient();
            _queue = client.GetQueueReference(queueName);
            _queue.EncodeMessage = encodeMessage;
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);
            _dequeueSemaphore = new SemaphoreSlim(1, 1);
        }

        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            var message = CreateMessage(item);
            await _queue.AddMessageAsync(message, null, null, null, null, cancellationToken);
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);

            // Synchronize the dequeue loop in a semaphore to reduce
            // overhead in concurrent scenarios
            await _dequeueSemaphore.WaitAsync(cancellationToken);

            var tryCount = 0;
            var delay = _minDequeueRetryDelay;            
            
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var item = await DequeueOrDefaultAsync(cancellationToken);
                    if (item != null)
                    {
                        return item;
                    }

                    await Task.Delay(delay, cancellationToken);
                    tryCount++;

                    if (delay < _maxDequeueRetryDelay)
                    {
                        delay = _minDequeueRetryDelay * (int) Math.Pow(2, tryCount);
                        if (delay > _maxDequeueRetryDelay)
                        {
                            delay = _maxDequeueRetryDelay;
                        }
                    }
                }
            }
            finally
            {
                _dequeueSemaphore.Release();
            }
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var message = await _queue.GetMessageAsync(cancellationToken);
            if (message == null) return default(T);

            return await CreateItemAndDeleteMessageAsync(message, cancellationToken);
        }

        public virtual async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize,
            CancellationToken cancellationToken)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            var messages = await _queue.GetMessagesAsync(maxBatchSize, cancellationToken);
            if (messages == null) return Enumerable.Empty<T>();

            return await Task.WhenAll(
                messages.Select(m => CreateItemAndDeleteMessageAsync(m, cancellationToken)));
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken);
            await _queue.FetchAttributesAsync(cancellationToken);
            return _queue.ApproximateMessageCount ?? 0;
        }

        async Task<StorageTransaction<T>> IBlockingReceiverQueue<StorageTransaction<T>>.DequeueAsync(
            CancellationToken cancellationToken)
        {
            var message = await _queue.GetMessageAsync(cancellationToken);
            if (message == null) return null;

            return CreateStorageTransaction(message);
        }

        async Task<IEnumerable<StorageTransaction<T>>> IBatchReceiverQueue<StorageTransaction<T>>.DequeueBatchAsync(
            int maxBatchSize,
            CancellationToken cancellationToken)
        {
            var messages = await _queue.GetMessagesAsync(maxBatchSize, cancellationToken);
            if (messages == null) return Enumerable.Empty<StorageTransaction<T>>();

            return messages.Select(CreateStorageTransaction);
        }

        public Task CommitAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken)
        {
            var message = CreateCloudQueueMessage(transaction);
            return _queue.DeleteMessageAsync(message, cancellationToken);
        }

        public Task RollbackAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken)
        {
            var message = CreateCloudQueueMessage(transaction);

            return _queue.UpdateMessageAsync(message, TimeSpan.Zero, MessageUpdateFields.Visibility, cancellationToken);
        }

        protected virtual CloudQueueMessage CreateMessage(T item)
        {
            var serializedItem = _serializer.Serialize(item);

            return new CloudQueueMessage(serializedItem);
        }

        protected virtual async Task<T> CreateItemAndDeleteMessageAsync(CloudQueueMessage message,
            CancellationToken cancellationToken)
        {
            var item = CreateItem(message);
            await _queue.DeleteMessageAsync(message, cancellationToken);
            return item;
        }

        protected virtual StorageTransaction<T> CreateStorageTransaction(CloudQueueMessage message)
        {
            var item = CreateItem(message);
            return new StorageTransaction<T>(message, item);
        }

        private async Task CreateQueueIfNotExistsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_queueExists) return;

            await _queueCreationSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (_queueExists) return;

                await _queue.CreateIfNotExistsAsync(cancellationToken);

                _queueExists = true;
            }
            finally
            {
                _queueCreationSemaphore.Release();
            }
        }

        private T CreateItem(CloudQueueMessage message)
        {
            var item = _serializer.Deserialize(message.AsString);
            return item;
        }

        private static CloudQueueMessage CreateCloudQueueMessage(StorageTransaction<T> transaction)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));
            if (!(transaction.Transaction is CloudQueueMessage cloudQueueMessage))
            {
                throw new ArgumentException("Invalid transaction type", nameof(transaction));
            }

            return cloudQueueMessage;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _queueCreationSemaphore?.Dispose();
                _dequeueSemaphore?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}