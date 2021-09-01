using AutoFixture;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class SenderReceiverQueueFacts<T> : FactsBase, IDisposable
    {
        private readonly CancellationTokenSource _cts;

        public SenderReceiverQueueFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        public abstract (IStreamSenderQueue<T>, IBlockingReceiverQueue<T>) Create();

        public CancellationToken CancellationToken => _cts.Token;

        protected virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = nameof(EnqueueNewItemSucceeds))]
        public virtual async Task EnqueueNewItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var item = CreateItem();

            // Act
            await senderQueue.EnqueueAsync(item, CancellationToken);

            // Assert
            AssertEquals(await receiverQueue.DequeueAsync(CancellationToken), item);
        }

        [Fact(DisplayName = nameof(EnqueueNewItemWithIdSucceeds))]
        public virtual async Task EnqueueNewItemWithIdSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var item = CreateItem();
            var id = Guid.NewGuid().ToString();

            // Act
            await senderQueue.EnqueueAsync(item, id, CancellationToken);

            // Assert
            AssertEquals(await receiverQueue.DequeueAsync(CancellationToken), item);
        }

        [Fact(DisplayName = nameof(EnqueueExistingItemSucceeds))]
        public virtual async Task EnqueueExistingItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var item = CreateItem();

            // Act
            await senderQueue.EnqueueAsync(item, CancellationToken);
            await senderQueue.EnqueueAsync(item, CancellationToken);

            // Assert
            AssertEquals(await receiverQueue.DequeueAsync(CancellationToken), item);
            AssertEquals(await receiverQueue.DequeueAsync(CancellationToken), item);
        }

        [Fact(DisplayName = nameof(EnqueueMultipleItemsSucceeds))]
        public virtual async Task EnqueueMultipleItemsSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var items = new ConcurrentBag<T>();
            var count = 100;
            for (int i = 0; i < count; i++)
            {
                var item = CreateItem();
                items.Add(item);
            }

            // Act
            var enumerator = items.GetEnumerator();
            var tasks = Enumerable
                .Range(0, count)
                .Where(_ => enumerator.MoveNext())
                .Select(_ => Task.Run(async () => await senderQueue.EnqueueAsync(enumerator.Current, CancellationToken)));

            await Task.WhenAll(tasks);

            // Assert

            foreach (var itemBag in items)
            {
                var item = await receiverQueue.DequeueAsync(CancellationToken);
                if (item == null)
                {
                    break;
                }

                AssertIsTrue(items.Contains(item));
            }
        }

        [Fact(DisplayName = nameof(DequeueEmptyReturnsDefault))]
        public virtual async Task DequeueEmptyReturnsDefault()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();

            // Act
            T actual;

            try
            {
                actual = await receiverQueue.DequeueAsync(CancellationToken);
            }
            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
            {
                actual = default;
            }

            // Assert
            AssertIsDefault(actual);
        }

        [Fact(DisplayName = nameof(DequeueMultipleItemsInParallelSucceeds))]
        public virtual async Task DequeueMultipleItemsInParallelSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var items = new HashSet<T>();
            var count = 100;
            for (var i = 0; i < count; i++)
            {
                var item = CreateItem();
                items.Add(item);
                await senderQueue.EnqueueAsync(item, CancellationToken);
            }

            var timeout = TimeSpan.FromMilliseconds(500);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = new ConcurrentBag<T>();
            var tasks = Enumerable
                .Range(0, count)
                .Select(i => Task.Run(async () => actualItems.Add(await receiverQueue.DequeueAsync(CancellationToken))));

            await Task.WhenAll(tasks);

            // Assert
            foreach (var item in items)
            {
                AssertIsTrue(actualItems.Contains(item));
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cts?.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}