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
    public abstract class QueueFacts<T> : FactsBase
    {
        public abstract IQueue<T> Create();

        [Fact(DisplayName = "EnqueueNewItemSucceeds")]
        public virtual async Task EnqueueNewItemSucceeds()
        {
            // Arrange
            var queue = Create();
            var item = Fixture.Create<T>();

            // Act
            await queue.EnqueueAsync(item);

            // Assert
            AssertEquals(await queue.GetLengthAsync(), 1);
            AssertEquals(await queue.DequeueOrDefaultAsync(), item);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "EnqueueExistingItemSucceeds")]
        public virtual async Task EnqueueExistingItemSucceeds()
        {
            // Arrange
            var queue = Create();
            var item = Fixture.Create<T>();
            await queue.EnqueueAsync(item);

            // Act
            await queue.EnqueueAsync(item);

            // Assert
            AssertEquals(await queue.GetLengthAsync(), 2);
            AssertEquals(await queue.DequeueOrDefaultAsync(), item);
            AssertEquals(await queue.DequeueOrDefaultAsync(), item);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }


        [Fact(DisplayName = "EnqueueMultipleItemsSucceeds")]
        public virtual async Task EnqueueMultipleItemsSucceeds()
        {
            // Arrange
            var queue = Create();
            var items = new ConcurrentBag<T>();
            var count = 100;
            for (int i = 0; i < count; i++)
            {
                var item = Fixture.Create<T>();
                items.Add(item);
            }

            // Act                           
            var enumerator = items.GetEnumerator();
            var tasks = Enumerable
                .Range(0, count)
                .Where(i => enumerator.MoveNext())
                .Select(i => Task.Run(async () => await queue.EnqueueAsync(enumerator.Current)));

            await Task.WhenAll(tasks);

            // Assert
            AssertEquals(await queue.GetLengthAsync(), count);
            while (await queue.GetLengthAsync() > 0)
            {
                var item = await queue.DequeueOrDefaultAsync();
                AssertIsTrue(items.Contains(item));
            }                        
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueEmptyReturnsDefault")]
        public virtual async Task DequeueEmptyReturnsDefault()
        {
            // Arrange
            var queue = Create();

            // Act
            var actual = await queue.DequeueOrDefaultAsync();

            // Assert
            AssertIsDefault(actual);
            AssertEquals(await queue.GetLengthAsync(), 0);
        }


        [Fact(DisplayName = "DequeueMultipleItemsInParallelSucceeds")]
        public virtual async Task DequeueMultipleItemsInParallelSucceeds()
        {
            // Arrange
            var queue = Create();
            var items = new HashSet<T>(); ;
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
                .Select(i => Task.Run(async () => actualItems.Add(await queue.DequeueOrDefaultAsync())));

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
