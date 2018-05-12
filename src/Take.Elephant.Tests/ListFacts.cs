using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class ListFacts<T> : FactsBase
    {
        public abstract IList<T> Create();

        public virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }


        [Fact(DisplayName = nameof(AddNewItemSucceeds))]
        public virtual async Task AddNewItemSucceeds()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();

            // Act
            await list.AddAsync(item);

            // Assert
            AssertEquals(await list.GetLengthAsync(), 1);
        }

        [Fact(DisplayName = nameof(AddExistingItemSucceeds))]
        public virtual async Task AddExistingItemSucceeds()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            await list.AddAsync(item);

            // Act
            await list.AddAsync(item);

            // Assert
            AssertEquals(await list.GetLengthAsync(), 2);
        }

        [Fact(DisplayName = nameof(RemoveAllExistingItemSucceeds))]
        public virtual async Task RemoveAllExistingItemSucceeds()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            await list.AddAsync(item);
            await list.AddAsync(item);
            await list.AddAsync(item);

            // Act
            var result = await list.RemoveAllAsync(item);

            // Assert
            AssertEquals(result, 3);
            AssertEquals(await list.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = nameof(RemoveAllNonExistingReturnsZero))]
        public virtual async Task RemoveAllNonExistingReturnsZero()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            await list.AddAsync(item);
            await list.AddAsync(item);
            await list.AddAsync(item);
            var nonExistingItem = CreateItem();

            // Act
            var result = await list.RemoveAllAsync(nonExistingItem);

            // Assert
            AssertEquals(result, 0);
            AssertEquals(await list.GetLengthAsync(), 3);
        }

        [Fact(DisplayName = nameof(EnumerateExistingItemsSucceeds))]
        public virtual async Task EnumerateExistingItemsSucceeds()
        {
            // Arrange
            var list = Create();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await list.AddAsync(item1);
            await list.AddAsync(item2);
            await list.AddAsync(item3);

            // Act
            var result = await list.AsEnumerableAsync();

            // Assert
            AssertEquals(await result.CountAsync(), 3);
            AssertEquals(await list.GetLengthAsync(), 3);
            AssertIsTrue(await result.ContainsAsync(item1));
            AssertIsTrue(await result.ContainsAsync(item2));
            AssertIsTrue(await result.ContainsAsync(item3));
        }
    }
}
