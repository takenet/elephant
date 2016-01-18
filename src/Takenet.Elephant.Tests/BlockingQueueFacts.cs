using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class BlockingQueueFacts<T> : QueueFacts<T>
    {
        public override abstract IQueue<T> Create();

        [Fact(DisplayName = "DequeueExistingItemSucceeds")]
        public virtual async Task DequeueExistingItemSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = Fixture.Create<T>();
            await queue.EnqueueAsync(item);
            var timeout = TimeSpan.FromMilliseconds(100);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actual = await queue.DequeueAsync(cts.Token);

            // Assert
            AssertEquals(actual, item);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueEmptyQueueThrowsTaskCanceledException")]
        public virtual async Task DequeueEmptyQueueThrowsTaskCanceledException()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var timeout = TimeSpan.FromMilliseconds(100);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            await AssertThrowsAsync<TaskCanceledException>(() =>
                queue.DequeueAsync(cts.Token));                        
        }

        [Fact(DisplayName = "DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds")]
        public virtual async Task DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = Fixture.Create<T>();
            var timeout = TimeSpan.FromMilliseconds(500);
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

        [Fact(DisplayName = "DequeueTwiceWithSingleItemThrowsTaskCanceledException")]
        public virtual async Task DequeueTwiceWithSingleItemThrowsTaskCanceledException()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var item = Fixture.Create<T>();
            await queue.EnqueueAsync(item);            
            var timeout = TimeSpan.FromMilliseconds(50);
            var cts = new CancellationTokenSource(timeout);            
            await queue.DequeueAsync(cts.Token);
            
            // Act
            await AssertThrowsAsync<TaskCanceledException>(() =>
                queue.DequeueAsync(cts.Token));
        }

        [Fact(DisplayName = "DequeueMultipleItemsInParallelSucceeds")]
        public virtual async Task DequeueMultipleItemsInParallelSucceeds()
        {
            // Arrange
            var queue = (IBlockingQueue<T>)Create();
            var items = new HashSet<T>();;
            var count = 100;
            for (var i = 0; i < count; i++)
            {
                var item = Fixture.Create<T>();
                items.Add(item);
                await queue.EnqueueAsync(item);
            }
            
            var timeout = TimeSpan.FromMilliseconds(500);
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
