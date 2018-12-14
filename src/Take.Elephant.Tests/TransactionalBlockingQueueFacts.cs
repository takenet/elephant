using System;
using System.Threading;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Shouldly;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class TransactionalBlockingQueueFacts<T> : FactsBase
    {
        public abstract IBlockingQueue<T> Create();

        protected virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }
        
        [Fact(DisplayName = nameof(DequeueAndCommitShouldRemove))]
        public virtual async Task DequeueAndCommitShouldRemove()
        {
            // Arrange
            var queue = Create();
            if (!(queue is ITransactionalStorage<T> transactionStorage))
            {
                throw new Exception($"Queue must implement {nameof(ITransactionalStorage<T>)}");
            }
            if (!(queue is IBlockingReceiverQueue<StorageTransaction<T>> receiverQueue))
            {
                throw new Exception($"Queue must implement {nameof(IBlockingReceiverQueue<StorageTransaction<T>>)}");
            }
            var item = CreateItem();
            await queue.EnqueueAsync(item);
            var timeout = TimeSpan.FromMilliseconds(5000);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actual = await receiverQueue.DequeueAsync(cts.Token);
            await transactionStorage.CommitAsync(actual, cts.Token);

            // Assert
            AssertEquals(await queue.GetLengthAsync(cts.Token), 0);
            actual.Item.ShouldBe(item);
        }

        [Fact(DisplayName = nameof(DequeueAndRollbackShouldKeep))]
        public virtual async Task DequeueAndRollbackShouldKeep()
        {
            // Arrange
            var queue = Create();
            if (!(queue is ITransactionalStorage<T> transactionStorage))
            {
                throw new Exception($"Queue must implement {nameof(ITransactionalStorage<T>)}");
            }
            if (!(queue is IBlockingReceiverQueue<StorageTransaction<T>> receiverQueue))
            {
                throw new Exception($"Queue must implement {nameof(IBlockingReceiverQueue<StorageTransaction<T>>)}");
            }
            var item = CreateItem();
            await queue.EnqueueAsync(item);
            var timeout = TimeSpan.FromMilliseconds(5000);
            var cts = new CancellationTokenSource(timeout);
            
            // Act
            var actual = await receiverQueue.DequeueAsync(cts.Token);
            await transactionStorage.RollbackAsync(actual, cts.Token);

            // Assert
            AssertEquals(await queue.GetLengthAsync(cts.Token), 1);
            actual.Item.ShouldBe(item);
            var existingItem = await queue.DequeueAsync(cts.Token);
            existingItem.ShouldBe(item);
        }
    }
}