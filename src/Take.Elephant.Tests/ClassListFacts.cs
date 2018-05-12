using System;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class ClassListFacts<T> : ListFacts<T> where T : class
    {
        [Fact(DisplayName = nameof(AddNullItemThrowsArgumentNullException))]
        public virtual async Task AddNullItemThrowsArgumentNullException()
        {
            // Arrange
            var list = Create();
            T item = null;

            // Act
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await list.AddAsync(item));
        }

        [Fact(DisplayName = nameof(RemoveAllNullItemThrowsArgumentNullException))]
        public virtual async Task RemoveAllNullItemThrowsArgumentNullException()
        {
            // Arrange
            var list = Create();
            T item = null;

            // Act
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await list.RemoveAllAsync(item));
        }
    }
}