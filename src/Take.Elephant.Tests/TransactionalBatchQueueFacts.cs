using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Shouldly;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class TransactionalBatchQueueFacts<T> : FactsBase
    {
        public abstract IBatchReceiverQueue<T> Create(params T[] items);

        protected virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }
 
        [Fact(DisplayName = nameof(DequeueBatchAndCommitShouldRemove))]
        public virtual async Task DequeueBatchAndCommitShouldRemove()
        {
            // Arrange
            var batchSize = 20;
            var items = Enumerable.Range(0, batchSize).Select(i => CreateItem()).ToArray();
            var batchReceiverQueue = Create(items);
            if (!(batchReceiverQueue is ITransactionalStorage<T> transactionStorage))
            {
                throw new Exception($"Queue must implement {nameof(ITransactionalStorage<T>)}");
            }
            if (!(batchReceiverQueue is IBatchReceiverQueue<StorageTransaction<T>> receiverQueue))
            {
                throw new Exception($"Queue must implement {nameof(IBlockingReceiverQueue<StorageTransaction<T>>)}");
            }
            var queue = (IQueue<T>) batchReceiverQueue;
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actualItems = await receiverQueue.DequeueBatchAsync(batchSize, cts.Token);
            await Task.WhenAll(
                actualItems.Select(i => transactionStorage.CommitAsync(i, cts.Token)));

            // Assert
            AssertEquals(await queue.GetLengthAsync(cts.Token), 0);
            AssertEquals(actualItems.Count(), batchSize);
            Check.That(actualItems.Select(i => i.Item)).ContainsExactly(items);
        }

        [Fact(DisplayName = nameof(DequeueBatchAndRollbackShouldKeep))]
        public virtual async Task DequeueBatchAndRollbackShouldKeep()
        {
            // Arrange
            var batchSize = 20;
            var items = Enumerable.Range(0, batchSize).Select(i => CreateItem()).ToArray();
            var batchReceiverQueue = Create(items);
            if (!(batchReceiverQueue is ITransactionalStorage<T> transactionStorage))
            {
                throw new Exception($"Queue must implement {nameof(ITransactionalStorage<T>)}");
            }
            if (!(batchReceiverQueue is IBatchReceiverQueue<StorageTransaction<T>> receiverQueue))
            {
                throw new Exception($"Queue must implement {nameof(IBlockingReceiverQueue<StorageTransaction<T>>)}");
            }

            var queue = (IQueue<T>) batchReceiverQueue;
            var timeout = TimeSpan.FromMilliseconds(30000);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actualItems = await receiverQueue.DequeueBatchAsync(batchSize, cts.Token);
            await Task.WhenAll(
                actualItems.Select(i => transactionStorage.RollbackAsync(i, cts.Token)));

            // Assert
            AssertEquals(await queue.GetLengthAsync(cts.Token), batchSize);
            AssertEquals(actualItems.Count(), batchSize);
            Check.That(actualItems.Select(i => i.Item)).ContainsExactly(items);
        }        
    }
}