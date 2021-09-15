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
    public abstract class PartitionedSenderReceiverFacts<TKey, TEvent> : FactsBase, IDisposable
    {
        private readonly CancellationTokenSource _cts;

        public PartitionedSenderReceiverFacts()
        {
            _cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        }

        public abstract (IEventStreamPublisher<string, Item>, IEventStreamConsumer<string, Item>) CreateStream();

        public CancellationToken CancellationToken => _cts.Token;

        protected virtual Item CreateItem()
        {
            return Fixture.Create<Item>();
        }

        [Fact(DisplayName = nameof(PublishNewItemSucceeds))]
        public virtual async Task PublishNewItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = CreateStream();
            var item = CreateItem();
            var key = Guid.NewGuid().ToString();

            // Act
            await senderQueue.PublishAsync(key, item, CancellationToken);

            // Assert
            AssertEquals((key, item), await receiverQueue.ConsumeAsync(key, CancellationToken));
        }

        [Fact(DisplayName = nameof(EnqueueExistingItemSucceeds))]
        public virtual async Task EnqueueExistingItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = CreateStream();
            var item = CreateItem();
            var key = Guid.NewGuid().ToString();

            // Act
            await senderQueue.PublishAsync(key, item, CancellationToken);
            await senderQueue.PublishAsync(key, item, CancellationToken);

            // Assert
            AssertEquals((key, item), await receiverQueue.ConsumeAsync(key, CancellationToken));
            AssertEquals((key, item), await receiverQueue.ConsumeAsync(key, CancellationToken));
        }

        //[Fact(DisplayName = nameof(EnqueueMultipleItemsSucceeds))]
        //public virtual async Task EnqueueMultipleItemsSucceeds()
        //{
        //    // Arrange
        //    var (senderQueue, receiverQueue) = CreateStream();
        //    var items = new ConcurrentBag<T>();
        //    var count = 100;
        //    for (int i = 0; i < count; i++)
        //    {
        //        var item = CreateItem();
        //        items.Add(item);
        //    }

        //    // Act
        //    var enumerator = items.GetEnumerator();
        //    var tasks = Enumerable
        //        .Range(0, count)
        //        .Where(_ => enumerator.MoveNext())
        //        .Select(_ => Task.Run(async () => await senderQueue.EnqueueAsync(enumerator.Current, CancellationToken)));

        //    await Task.WhenAll(tasks);

        //    // Assert

        //    foreach (var itemBag in items)
        //    {
        //        var item = await receiverQueue.DequeueAsync(CancellationToken);
        //        if (item == null)
        //        {
        //            break;
        //        }

        //        AssertIsTrue(items.Contains(item));
        //    }
        //}

        //[Fact(DisplayName = nameof(DequeueEmptyReturnsDefault))]
        //public virtual async Task DequeueEmptyReturnsDefault()
        //{
        //    // Arrange
        //    var (senderQueue, receiverQueue) = CreateStream();

        //    // Act
        //    T actual;

        //    try
        //    {
        //        actual = await receiverQueue.DequeueAsync(CancellationToken);
        //    }
        //    catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        //    {
        //        actual = default;
        //    }

        //    // Assert
        //    AssertIsDefault(actual);
        //}

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