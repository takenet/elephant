using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class SenderReceiverQueueFacts<T> : FactsBase
    {
        public abstract (ISenderQueue<T>, IReceiverQueue<T>) Create();

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
            await senderQueue.EnqueueAsync(item);

            // Assert
            AssertEquals(await receiverQueue.DequeueOrDefaultAsync(), item);
        }
        
        [Fact(DisplayName = nameof(EnqueueExistingItemSucceeds))]
        public virtual async Task EnqueueExistingItemSucceeds()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();
            var item = CreateItem();
            await senderQueue.EnqueueAsync(item);

            // Act
            await senderQueue.EnqueueAsync(item);

            // Assert
            AssertEquals(await receiverQueue.DequeueOrDefaultAsync(), item);
            AssertEquals(await receiverQueue.DequeueOrDefaultAsync(), item);
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
                .Where(i => enumerator.MoveNext())
                .Select(i => Task.Run(async () => await senderQueue.EnqueueAsync(enumerator.Current)));

            await Task.WhenAll(tasks);

            // Assert
            while (true)
            {
                var item = await receiverQueue.DequeueOrDefaultAsync();
                if (item == null) break;
                AssertIsTrue(items.Contains(item));
            }
        }

        [Fact(DisplayName = nameof(DequeueEmptyReturnsDefault))]
        public virtual async Task DequeueEmptyReturnsDefault()
        {
            // Arrange
            var (senderQueue, receiverQueue) = Create();

            // Act
            var actual = await receiverQueue.DequeueOrDefaultAsync();

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
                await senderQueue.EnqueueAsync(item);
            }

            var timeout = TimeSpan.FromMilliseconds(500);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = new ConcurrentBag<T>();
            var tasks = Enumerable
                .Range(0, count)
                .Select(i => Task.Run(async () => actualItems.Add(await receiverQueue.DequeueOrDefaultAsync())));

            await Task.WhenAll(tasks);

            // Assert
            foreach (var item in items)
            {
                AssertIsTrue(actualItems.Contains(item));
            }
        }        
    }
}