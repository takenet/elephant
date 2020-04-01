using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using NFluent;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class BatchReceiverQueueFacts<T> : FactsBase
    {
        public abstract IBatchReceiverQueue<T> Create(params T[] items);

        protected virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = nameof(DequeueBatchSucceeds))]
        public virtual async Task DequeueBatchSucceeds()
        {
            // Arrange
            var batchSize = 20;
            var items = Enumerable.Range(0, batchSize).Select(i => CreateItem()).ToArray();
            var batchReceiverQueue = Create(items);
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = await batchReceiverQueue.DequeueBatchAsync(batchSize, cts.Token);

            // Assert
            AssertEquals(actualItems.Count(), batchSize);
            Check.That(actualItems).ContainsExactly(items);
        }

        [Fact(DisplayName = nameof(DequeueLessThanExistingReturnsRequested))]
        public virtual async Task DequeueLessThanExistingReturnsRequested()
        {
            // Arrange
            var batchSize = 20;
            var dequeueBatchSize = 10;
            var items = Enumerable.Range(0, batchSize).Select(i => CreateItem()).ToArray();
            var batchReceiverQueue = Create(items);
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = await batchReceiverQueue.DequeueBatchAsync(dequeueBatchSize, cts.Token);

            // Assert
            AssertEquals(actualItems.Count(), dequeueBatchSize);
            Check.That(actualItems).ContainsExactly(items.Take(dequeueBatchSize));
        }

        [Fact(DisplayName = nameof(DequeueMoreThanExistingReturnsExisting))]
        public virtual async Task DequeueMoreThanExistingReturnsExisting()
        {
            // Arrange
            var batchSize = 10;
            var dequeueBatchSize = 20;
            var items = Enumerable.Range(0, batchSize).Select(i => CreateItem()).ToArray();
            var batchReceiverQueue = Create(items);
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = await batchReceiverQueue.DequeueBatchAsync(dequeueBatchSize, cts.Token);

            // Assert
            AssertEquals(actualItems.Count(), batchSize);
            Check.That(actualItems).ContainsExactly(items);
        }

        [Fact(DisplayName = nameof(DequeueEmptyQueueReturnsEmpty))]
        public virtual async Task DequeueEmptyQueueReturnsEmpty()
        {
            // Arrange
            var batchSize = 20;            
            var batchReceiverQueue = Create();
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);

            // Act
            var actualItems = await batchReceiverQueue.DequeueBatchAsync(batchSize, cts.Token);

            // Assert            
            Check.That(actualItems).IsEmpty();
        }
    }
}
