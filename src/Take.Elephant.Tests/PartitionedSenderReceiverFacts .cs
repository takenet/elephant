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
            await DelayTestAsync();

            // Assert
            AssertEquals((key, item), await receiverQueue.ConsumeOrDefaultAsync(CancellationToken));
        }

        [Fact(DisplayName = nameof(PublishExistingItemSucceeds))]
        public virtual async Task PublishExistingItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = CreateStream();
            var item = CreateItem();
            var key = Guid.NewGuid().ToString();

            // Act
            await senderQueue.PublishAsync(key, item, CancellationToken);
            await senderQueue.PublishAsync(key, item, CancellationToken);
            await DelayTestAsync();

            // Assert
            AssertEquals((key, item), await receiverQueue.ConsumeOrDefaultAsync(CancellationToken));
            AssertEquals((key, item), await receiverQueue.ConsumeOrDefaultAsync(CancellationToken));
        }        

        [Fact(DisplayName = nameof(PublishMultipleItemsSucceeds))]
        public virtual async Task PublishMultipleItemsSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = CreateStream();
            var items = new ConcurrentBag<Tuple<string,Item>>();
            var count = 10;
            for (int i = 0; i < count; i++)
            {
                var item = CreateItem();
                var key = Guid.NewGuid().ToString();
                items.Add(new Tuple<string, Item>(key,item));
            }

            // Act
            var enumerator = items.GetEnumerator();
            var tasks = Enumerable
                .Range(0, count)
                .Where(_ => enumerator.MoveNext())
                .Select(_ => Task.Run(async () => await senderQueue.PublishAsync(enumerator.Current.Item1, enumerator.Current.Item2,  CancellationToken)));

            await Task.WhenAll(tasks);
            await DelayTestAsync();

            // Assert

            foreach (var itemBag in items)
            {
                var item = await receiverQueue.ConsumeOrDefaultAsync(CancellationToken);
                if (item.key == null)
                {
                    break;
                }

                AssertIsTrue(items.Where(i => i.Item1 == item.key).Any());
            }
        }

        [Fact(DisplayName = nameof(ConsumeEmptyReturnsDefault))]
        public virtual void ConsumeEmptyReturnsDefault()
        {
            // Arrange
            var (senderQueue, receiverQueue) = CreateStream();

            // Act
            Item actual;

            try
            {
                actual = receiverQueue.ConsumeOrDefaultAsync(CancellationToken).Result.item;
            }
            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
            {
                actual = default;
            }

            // Assert
            AssertIsDefault(actual);
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

        private static async Task DelayTestAsync() => await Task.Delay(4000);
    }
}