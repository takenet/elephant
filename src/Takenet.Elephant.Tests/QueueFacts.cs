using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
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
    }
}
