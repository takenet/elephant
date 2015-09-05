using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class SetFacts<T> : FactsBase
    {
        public abstract ISet<T> Create();

        public virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = "AddNewItemSucceeds")]
        public virtual async Task AddNewItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = CreateItem();

            // Act
            await set.AddAsync(item);

            // Assert
            AssertIsTrue(await set.ContainsAsync(item));
            AssertEquals(await set.GetLengthAsync(), 1);
        }

        [Fact(DisplayName = "AddExistingItemSucceeds")]
        public virtual async Task AddExistingItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = CreateItem();
            await set.AddAsync(item);

            // Act
            await set.AddAsync(item);

            // Assert
            AssertIsTrue(await set.ContainsAsync(item));
            AssertEquals(await set.GetLengthAsync(), 1);
        }

        [Fact(DisplayName = "TryRemoveExistingItemSucceeds")]
        public virtual async Task TryRemoveExistingItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = CreateItem();
            await set.AddAsync(item);

            // Act
            var result = await set.TryRemoveAsync(item);

            // Assert
            AssertIsTrue(result);
            AssertIsFalse(await set.ContainsAsync(item));
            AssertEquals(await set.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "TryRemoveNonExistingItemFails")]
        public virtual async Task TryRemoveNonExistingItemFails()
        {
            // Arrange
            var set = Create();
            var item = CreateItem();

            // Act
            var result = await set.TryRemoveAsync(item);

            // Assert
            AssertIsFalse(result);
        }

        [Fact(DisplayName = "EnumerateExistingItemsSucceeds")]
        public virtual async Task EnumerateExistingItemsSucceeds()
        {
            // Arrange
            var set = Create();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await set.AddAsync(item1);
            await set.AddAsync(item2);
            await set.AddAsync(item3);

            // Act
            var result = await set.AsEnumerableAsync();

            // Assert
            AssertEquals(await result.CountAsync(), 3);
            AssertEquals(await set.GetLengthAsync(), 3);
            AssertIsTrue(await result.ContainsAsync(item1));
            AssertIsTrue(await result.ContainsAsync(item2));
            AssertIsTrue(await result.ContainsAsync(item3));            
        }

        [Fact(DisplayName = "EnumerateAfterRemovingItemsSucceeds")]
        public virtual async Task EnumerateAfterRemovingItemsSucceeds()
        {
            // Arrange
            var set = Create();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();
            await set.AddAsync(item1);
            await set.AddAsync(item2);
            await set.AddAsync(item3);

            // Act
            var result = await set.AsEnumerableAsync();
            await set.TryRemoveAsync(item1);
            await set.TryRemoveAsync(item2);
            await set.TryRemoveAsync(item3);

            // Assert
            AssertEquals(await result.CountAsync(), 0);
            AssertEquals(await set.GetLengthAsync(), 0);
            AssertIsFalse(await result.ContainsAsync(item1));
            AssertIsFalse(await result.ContainsAsync(item2));
            AssertIsFalse(await result.ContainsAsync(item3));
        }

        [Fact(DisplayName = "CheckForExistingItemSucceeds")]
        public virtual async Task CheckForExistingItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item1 = CreateItem();
            await set.AddAsync(item1);

            // Act
            var result = await set.ContainsAsync(item1);

            // Assert
            AssertIsTrue(result);
        }

        [Fact(DisplayName = "CheckForNonExistingItemFails")]
        public virtual async Task CheckForNonExistingItemFails()
        {
            // Arrange
            var set = Create();
            var item1 = CreateItem();
            var item2 = CreateItem();
            await set.AddAsync(item1);

            // Act
            var result = await set.ContainsAsync(item2);

            // Assert
            AssertIsFalse(result);
        }
    }
}
