using System;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class ClassSetFacts<T> : SetFacts<T> where T : class
    {
        [Fact(DisplayName = "AddNullItemThrowsArgumentNullException")]
        public virtual async Task AddNullItemThrowsArgumentNullException()
        {
            // Arrange
            var set = Create();
            T item = null;

            // Act
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await set.AddAsync(item));
        }

        [Fact(DisplayName = "TryRemoveNullItemThrowsArgumentNullException")]
        public virtual async Task TryRemoveNullItemThrowsArgumentNullException()
        {
            // Arrange
            var set = Create();
            T item = null;

            // Act
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await set.TryRemoveAsync(item));
        }

        [Fact(DisplayName = "CheckForExistingNullItemThrowsArgumentNullException")]
        public virtual async Task CheckForExistingNullItemThrowsArgumentNullException()
        {
            // Arrange
            var set = Create();
            T item = null;

            // Act
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await set.ContainsAsync(item));
        }
    }
}