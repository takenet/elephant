using System;
using System.Linq;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class PositionListFacts<T> : FactsBase
    {
        public abstract IPositionList<T> Create();

        public virtual T CreateItem()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = nameof(AddNewItemToHeadSucceeds))]
        public virtual async Task AddNewItemToHeadSucceeds()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            var item2 = CreateItem();

            // Act
            await list.AddAsync(item);
            await list.AddToPositionAsync(item2, 0);

            // Assert
            AssertEquals(await list.GetLengthAsync(), 2);
            var listEnumerable = await list.AsEnumerableAsync();
            AssertEquals(await listEnumerable.FirstAsync(), item2);
        }

        [Fact(DisplayName = nameof(AddNewItemToTailSucceds))]
        public virtual async Task AddNewItemToTailSucceds()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            var item2 = CreateItem();

            // Act
            await list.AddAsync(item);
            await list.AddToPositionAsync(item2, 1);

            // Assert
            AssertEquals(await list.GetLengthAsync(), 2);
            var listEnumerable = await list.AsEnumerableAsync();
            AssertEquals(listEnumerable.ToListAsync().Result.Last(), item2);
        }

        [Fact(DisplayName = nameof(AddNewItemToMiddleSucceds))]
        public virtual async Task AddNewItemToMiddleSucceds()
        {
            // Arrange
            var list = Create();
            var item1 = CreateItem();
            var item2 = CreateItem();
            var item3 = CreateItem();

            // Act
            await list.AddAsync(item1);
            await list.AddAsync(item3);
            await list.AddToPositionAsync(item2, 1);

            // Assert
            AssertEquals(await list.GetLengthAsync(), 3);
            var listEnumerable = await list.AsEnumerableAsync();
            AssertEquals(listEnumerable.ToListAsync().Result[1], item2);
        }

        [Fact(DisplayName = nameof(AddNewItemToInexistingIndexFails))]
        public virtual async Task AddNewItemToInexistingIndexFails()
        {
            // Arrange
            var list = Create();
            var item = CreateItem();
            var item2 = CreateItem();

            // Act
            await list.AddAsync(item);

            // Assert
            await AssertThrowsAsync<ArgumentOutOfRangeException>(() => list.AddToPositionAsync(item2, 5));
        }
    }
}