using Ploeh.AutoFixture;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class SortedSetFacts<T> : FactsBase
    {
        public abstract ISortedSet<T> Create();

        [Fact(DisplayName = "EnqueueNewItemSucceeds")]
        public virtual async Task EnqueueNewItemSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var item = Fixture.Create<T>();

            // Act
            await sortedSet.EnqueueAsync(item, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 1);
            AssertEquals(await sortedSet.DequeueMinOrDefaultAsync(), item);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "EnqueueOrderedExistingItemSucceeds")]
        public virtual async Task EnqueueOrderedExistingItemSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();

            // Act
            await sortedSet.EnqueueAsync(firstItem, 0.02);
            await sortedSet.EnqueueAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.DequeueMinOrDefaultAsync(), secondItem);
            AssertEquals(await sortedSet.DequeueMinOrDefaultAsync(), firstItem);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "RemoveExistingItemSucceeds")]
        public virtual async Task RemoveExistingItemSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();

            // Act
            await sortedSet.EnqueueAsync(firstItem, 0.02);
            await sortedSet.EnqueueAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.RemoveAsync(firstItem), true);
            AssertEquals(await sortedSet.GetLengthAsync(), 1);
            AssertEquals(await sortedSet.RemoveAsync(secondItem), true);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        public async Task<ISortedSet<T>> SetupRangeByRankTests()
        {
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.EnqueueAsync(firstItem, 0.01);
            await sortedSet.EnqueueAsync(secondItem, 0.02);
            await sortedSet.EnqueueAsync(thirdItem, 0.03);
            await sortedSet.EnqueueAsync(fourthItem, 0.04);

            return sortedSet;
        }

        [Fact(DisplayName = "RangeByRankTwoItensAsyncItensSucceeds")]
        public virtual async Task RangeByRankTwoItensAsyncItensSucceeds()
        {
            // Arrange
            var sortedSet = await SetupRangeByRankTests();

            // Act
            var rangedItens = await sortedSet.RangeByRankAsync(2, 4);

            // Assert
            AssertEquals(rangedItens.Count(), 2);
        }

        [Fact(DisplayName = "RangeByRankMoreIndexesOnlyOneItensSucceeds")]
        public virtual async Task RangeByRankMoreIndexesOnlyOneItensSucceeds()
        {
            // Arrange
            var sortedSet = await SetupRangeByRankTests();

            // Act
            var rangedItens = await sortedSet.RangeByRankAsync(3, 6);

            // Assert
            AssertEquals(rangedItens.Count(), 1);
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
                .Select(i => Task.Run(async () => await queue.EnqueueAsync(enumerator.Current, 0.01)));

            await Task.WhenAll(tasks);

            // Assert
            AssertEquals(await queue.GetLengthAsync(), count);
            while (await queue.GetLengthAsync() > 0)
            {
                var item = await queue.DequeueMinOrDefaultAsync();
                AssertIsTrue(items.Contains(item));
            }
            AssertEquals(await queue.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueEmptyReturnsDefault")]
        public virtual async Task DequeueEmptyReturnsDefault()
        {
            // Arrange
            var sortedSet = Create();

            // Act
            var actual = await sortedSet.DequeueMinOrDefaultAsync();

            // Assert
            AssertIsDefault(actual);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueOrderedExistingItemPerMinimumSucceeds")]
        public virtual async Task DequeueOrderedExistingItemPerMinimumSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();

            // Act
            await sortedSet.EnqueueAsync(firstItem, 0.02);
            await sortedSet.EnqueueAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.DequeueMinOrDefaultAsync(), secondItem);
            AssertEquals(await sortedSet.DequeueMinOrDefaultAsync(), firstItem);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueOrderedExistingItemPerMaximumSucceeds")]
        public virtual async Task DequeueOrderedExistingItemPerMaximumSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();

            // Act
            await sortedSet.EnqueueAsync(firstItem, 0.02);
            await sortedSet.EnqueueAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.DequeueMaxOrDefaultAsync(), firstItem);
            AssertEquals(await sortedSet.DequeueMaxOrDefaultAsync(), secondItem);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueMultipleItemsInParallelSucceeds")]
        public virtual async Task DequeueMultipleItemsInParallelSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var items = new HashSet<T>();
            var count = 100;
            for (var i = 0; i < count; i++)
            {
                var item = Fixture.Create<T>();
                items.Add(item);
                await sortedSet.EnqueueAsync(item, 0.01);
            }

            var timeout = TimeSpan.FromMilliseconds(500);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = new ConcurrentBag<T>();
            var tasks = Enumerable
                .Range(0, count)
                .Select(i => Task.Run(async () => actualItems.Add(await sortedSet.DequeueMinOrDefaultAsync())));

            await Task.WhenAll(tasks);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
            foreach (var item in items)
            {
                AssertIsTrue(actualItems.Contains(item));
            }
        }

        //Priority blocking queue tests
        //[Fact(DisplayName = "DequeueExistingItemSucceeds")]
        //public virtual async Task DequeueExistingItemSucceeds()
        //{
        //    // Arrange
        //    var queue = Create();
        //    var item = Fixture.Create<T>();
        //    await queue.EnqueueAsync(item, 0.01);
        //    var timeout = TimeSpan.FromMilliseconds(500);
        //    var cts = new CancellationTokenSource(timeout);

        //    // Act
        //    var actual = await queue.DequeueMinAsync(cts.Token);

        //    // Assert
        //    AssertEquals(actual, item);
        //    AssertEquals(await queue.GetLengthAsync(), 0);
        //}

        //[Fact(DisplayName = nameof(DequeueEmptyQueueThrowsOperationCanceledException))]
        //public virtual async Task DequeueEmptyQueueThrowsOperationCanceledException()
        //{
        //    // Arrange
        //    var queue = Create();
        //    var timeout = TimeSpan.FromMilliseconds(500);
        //    var cts = new CancellationTokenSource(timeout);

        //    // Act
        //    await AssertThrowsAsync<OperationCanceledException>(() =>
        //        queue.DequeueMinAsync(cts.Token));
        //}

        //[Fact(DisplayName = "DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds")]
        //public virtual async Task DequeueEmptyQueueAddingAfterDequeueWasCalledSucceeds()
        //{
        //    // Arrange
        //    var queue = Create();
        //    var item = Fixture.Create<T>();
        //    var timeout = TimeSpan.FromMilliseconds(500);
        //    var cts = new CancellationTokenSource(timeout + timeout);

        //    // Act
        //    var dequeueTask = queue.DequeueMinAsync(cts.Token);
        //    await Task.Delay(timeout);
        //    AssertIsFalse(dequeueTask.IsCompleted);
        //    await queue.EnqueueAsync(item, 0.01);
        //    var actual = await dequeueTask;

        //    // Assert
        //    AssertEquals(actual, item);
        //    AssertEquals(await queue.GetLengthAsync(), 0);
        //}

        //[Fact(DisplayName = nameof(DequeueTwiceWithSingleItemThrowsOperationCanceledException))]
        //public virtual async Task DequeueTwiceWithSingleItemThrowsOperationCanceledException()
        //{
        //    // Arrange
        //    var queue = Create();
        //    var item = Fixture.Create<T>();
        //    await queue.EnqueueAsync(item, 0.01);
        //    var timeout = TimeSpan.FromMilliseconds(500);
        //    var cts = new CancellationTokenSource(timeout);
        //    await queue.DequeueMinAsync(cts.Token);

        //    // Act
        //    await AssertThrowsAsync<OperationCanceledException>(() =>
        //        queue.DequeueMinAsync(cts.Token));
        //}

        //[Fact(DisplayName = "DequeueBlockingMultipleItemsInParallelSucceeds")]
        //public virtual async Task DequeueBlockingMultipleItemsInParallelSucceeds()
        //{
        //    // Arrange
        //    var queue = Create();
        //    var items = new List<T>();
        //    var count = 100;
        //    for (var i = 0; i < count; i++)
        //    {
        //        var item = Fixture.Create<T>();
        //        items.Add(item);
        //        await queue.EnqueueAsync(item, 0.01);
        //    }

        //    var timeout = TimeSpan.FromMilliseconds(30000);
        //    var cts = new CancellationTokenSource(timeout);

        //    // Act
        //    var actualItems = new ConcurrentBag<T>();
        //    var tasks = Enumerable
        //        .Range(0, count)
        //        .Select(i => Task.Run(async () => actualItems.Add(await queue.DequeueMinAsync(cts.Token))));

        //    await Task.WhenAll(tasks);

        //    // Assert
        //    AssertEquals(await queue.GetLengthAsync(), 0);
        //    foreach (var item in items)
        //    {
        //        AssertIsTrue(actualItems.Contains(item));
        //    }
        //}
    }
}