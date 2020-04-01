using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class BlockingQueueFacts<T> : QueueFacts<T>
    {
        public abstract override IQueue<T> Create();

        [Fact(DisplayName = "DequeueExistingItemSucceeds")]
        public virtual async Task DequeueExistingItemSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = CreateItem();
            await queue.EnqueueAsync(item);
            var timeout = TimeSpan.FromMilliseconds(500);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actual = await queue.DequeueAsync(cts.Token);

            // Assert
            AssertEquals(actual, item);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = nameof(DequeueEmptyQueueThrowsOperationCanceledException))]
        public virtual async Task DequeueEmptyQueueThrowsOperationCanceledException()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var timeout = TimeSpan.FromMilliseconds(5000);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            await AssertThrowsAsync<OperationCanceledException>(() =>
                queue.DequeueAsync(cts.Token));                        
        }

        [Fact(DisplayName = "DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds")]
        public virtual async Task DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = CreateItem();
            var timeout = TimeSpan.FromMilliseconds(3000);
            var cts = new CancellationTokenSource(timeout + timeout);

            // Act
            var dequeueTask = queue.DequeueAsync(cts.Token);
            await Task.Delay(timeout);
            AssertIsFalse(dequeueTask.IsCompleted);
            await queue.EnqueueAsync(item);
            var actual = await dequeueTask;

            // Assert
            AssertEquals(actual, item);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = nameof(DequeueTwiceWithSingleItemThrowsOperationCanceledException))]
        public virtual async Task DequeueTwiceWithSingleItemThrowsOperationCanceledException()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = CreateItem();
            await queue.EnqueueAsync(item);            
            var timeout = TimeSpan.FromMilliseconds(500);
            var cts = new CancellationTokenSource(timeout);            
            await queue.DequeueAsync(cts.Token);
            
            // Act
            await AssertThrowsAsync<OperationCanceledException>(() =>
                queue.DequeueAsync(cts.Token));
        }

        [Fact(DisplayName = "DequeueBlockingMultipleItemsInParallelSucceeds")]
        public virtual async Task DequeueBlockingMultipleItemsInParallelSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var items = new List<T>();
            var count = 100;
            for (var i = 0; i < count; i++)
            {
                var item = CreateItem();
                items.Add(item);
                await queue.EnqueueAsync(item);
            }
            
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = new ConcurrentBag<T>();
            var tasks = Enumerable
                .Range(0, count)
                .Select(i => Task.Run(async () => actualItems.Add(await queue.DequeueAsync(cts.Token))));

            await Task.WhenAll(tasks);

            // Assert
            AssertEquals(await queue.GetLengthAsync(), 0);
            foreach (var item in items)
            {
                AssertIsTrue(actualItems.Contains(item));
            }            
        }
    }
}
