using Dawn;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Take.Elephant.Adapters;
using Azure.Storage.Queues.Models;

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
        private readonly QueueClient _queue;
        private readonly ISerializer<T> _serializer;
        private readonly TimeSpan? _visibilityTimeout;
        private readonly SemaphoreSlim _queueCreationSemaphore;
        private readonly PollingBlockingQueueAdapter<T> _pollingBlockingQueueAdapter;

        private bool _queueExists;

        /// <summary>
        /// Creates a Azure Storage Account used as Queue.
        /// </summary>
        /// <param name="storageConnectionString"></param>
        /// <param name="queueName"></param>
        /// <param name="serializer"></param>
        /// <param name="encodeMessage"></param>
        /// <param name="minDequeueRetryDelay"></param>
        /// <param name="maxDequeueRetryDelay"></param>
        /// <param name="visibilityTimeout">Optional. Specifies the new visibility timeout value, in seconds, relative to
        ///     server time. The default value is 30 seconds.</param>
        public AzureStorageQueue(
            string storageConnectionString,
            string queueName,
            ISerializer<T> serializer,
            bool encodeMessage = true,
            int minDequeueRetryDelay = 250,
            int maxDequeueRetryDelay = 30000,
            TimeSpan? visibilityTimeout = default)
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
            _visibilityTimeout = visibilityTimeout;
            var options = new QueueClientOptions();
            if (encodeMessage)
            {
                options.MessageEncoding = QueueMessageEncoding.Base64;
            }

            _queue = new QueueClient(storageConnectionString, queueName, options);
            _queueCreationSemaphore = new SemaphoreSlim(1, 1);
            _pollingBlockingQueueAdapter = new PollingBlockingQueueAdapter<T>(this, minDequeueRetryDelay, maxDequeueRetryDelay, 1);
        }

        public virtual async Task EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken).ConfigureAwait(false);

            var message = CreateMessage(item);
            await _queue.SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
        }

        public virtual async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken).ConfigureAwait(false);

            return await _pollingBlockingQueueAdapter.DequeueAsync(cancellationToken).ConfigureAwait(false);
        }

        public virtual async Task<T> DequeueOrDefaultAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
            var message = await _queue.ReceiveMessageAsync(_visibilityTimeout, cancellationToken).ConfigureAwait(false);
            if (message?.Value == null)
                return default;

            return await CreateItemAndDeleteMessageAsync(message.Value, cancellationToken).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> DequeueBatchAsync(int maxBatchSize,
            CancellationToken cancellationToken)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
            var messages = await _queue.ReceiveMessagesAsync(maxBatchSize, _visibilityTimeout, cancellationToken).ConfigureAwait(false);
            if (messages?.Value == null)
                return Enumerable.Empty<T>();

            return await Task.WhenAll(
                messages.Value.Select(m => CreateItemAndDeleteMessageAsync(m, cancellationToken))).ConfigureAwait(false);
        }

        public virtual async Task<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            await CreateQueueIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
            var properties = await _queue.GetPropertiesAsync(cancellationToken).ConfigureAwait(false);

            return properties.Value?.ApproximateMessagesCount ?? 0;
        }

        async Task<StorageTransaction<T>> IBlockingReceiverQueue<StorageTransaction<T>>.DequeueAsync(
            CancellationToken cancellationToken)
        {
            var message = await _queue.ReceiveMessageAsync(_visibilityTimeout, cancellationToken).ConfigureAwait(false);
            if (message == null)
                return null;

            return CreateStorageTransaction(message);
        }

        async Task<IEnumerable<StorageTransaction<T>>> IBatchReceiverQueue<StorageTransaction<T>>.DequeueBatchAsync(
            int maxBatchSize,
            CancellationToken cancellationToken)
        {
            var messages = await _queue.ReceiveMessagesAsync(maxBatchSize, _visibilityTimeout, cancellationToken).ConfigureAwait(false);
            if (messages == null)
                return Enumerable.Empty<StorageTransaction<T>>();

            return messages.Value.Select(CreateStorageTransaction);
        }

        public Task CommitAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken)
        {
            var message = CreateCloudQueueMessage(transaction);
            return _queue.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken);
        }

        public Task RollbackAsync(StorageTransaction<T> transaction, CancellationToken cancellationToken)
        {
            var message = CreateCloudQueueMessage(transaction);

            return _queue.UpdateMessageAsync(message.MessageId, message.PopReceipt, message: null, TimeSpan.Zero, cancellationToken);
        }

        protected virtual string CreateMessage(T item)
            => _serializer.Serialize(item);

        protected virtual async Task<T> CreateItemAndDeleteMessageAsync(QueueMessage message,
            CancellationToken cancellationToken)
        {
            var item = CreateItem(message);
            await _queue.DeleteMessageAsync(message.MessageId, message.PopReceipt, cancellationToken).ConfigureAwait(false);
            return item;
        }

        protected virtual StorageTransaction<T> CreateStorageTransaction(QueueMessage message)
        {
            var item = CreateItem(message);
            return new StorageTransaction<T>(message, item);
        }

        private async Task CreateQueueIfNotExistsAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_queueExists)
                return;

            await _queueCreationSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_queueExists)
                    return;

                await _queue.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

                _queueExists = true;
            }
            finally
            {
                _queueCreationSemaphore.Release();
            }
        }

        private T CreateItem(QueueMessage message)
        {
            var item = _serializer.Deserialize(message.MessageText);
            return item;
        }

        private static QueueMessage CreateCloudQueueMessage(StorageTransaction<T> transaction)
        {
            if (transaction == null)
                throw new ArgumentNullException(nameof(transaction));
            if (!(transaction.Transaction is QueueMessage cloudQueueMessage))
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
                _pollingBlockingQueueAdapter.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}