using AutoFixture;
using System.Collections.Generic;
using System.Linq;
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
            await sortedSet.AddAsync(item, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 1);
            AssertEquals(await sortedSet.RemoveMinOrDefaultAsync(), item);
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
            await sortedSet.AddAsync(firstItem, 0.02);
            await sortedSet.AddAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.RemoveMinOrDefaultAsync(), secondItem);
            AssertEquals(await sortedSet.RemoveMinOrDefaultAsync(), firstItem);
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
            await sortedSet.AddAsync(firstItem, 0.02);
            await sortedSet.AddAsync(secondItem, 0.01);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
            AssertEquals(await sortedSet.RemoveAsync(firstItem), true);
            AssertEquals(await sortedSet.GetLengthAsync(), 1);
            AssertEquals(await sortedSet.RemoveAsync(secondItem), true);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "RemoveEmptyFails")]
        public virtual async Task RemoveEmptyFails()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();

            // Act
            var result = await sortedSet.RemoveAsync(firstItem);

            // Assert
            AssertEquals(result, false);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "RemoveNonExistingItemFails")]
        public virtual async Task RemoveNonExistingItemFails()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();

            // Act
            await sortedSet.AddAsync(firstItem, 0.02);
            var result = await sortedSet.RemoveAsync(secondItem);

            // Assert
            AssertEquals(result, false);
            AssertEquals(await sortedSet.GetLengthAsync(), 1);
        }

        [Fact(DisplayName = "RangeByScoreTwoItensAsyncItensSucceeds")]
        public virtual async Task RangeByScoreTwoItensAsyncItensSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByScoreAsync(0.02, 0.03);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 2);
            AssertEquals(rangedItens.FirstOrDefaultAsync().Result, secondItem);
            AssertEquals(rangedItens.ToListAsync().Result.Last(), thirdItem);
        }

        [Fact(DisplayName = "RangeByScoreOneItemAsyncSucceeds")]
        public virtual async Task RangeByScoreOneItensAsyncSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByScoreAsync(0.02, 0.02);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 1);
            AssertEquals(rangedItens.ToListAsync().Result.FirstOrDefault(), secondItem);
        }

        [Fact(DisplayName = "RangeByScoreWithMultipleItensSameScoreSucceeds")]
        public virtual async Task RangeByScoreWithMultipleItensSameScoreSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.02);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByScoreAsync(0.02, 0.02);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 2);
        }

        [Fact(DisplayName = "RangeByScoreOutOfRangeSucceeds")]
        public virtual async Task RangeByScoreOutOfRangeSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.02);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByScoreAsync(1, 2);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 0);
        }

        [Fact(DisplayName = "RangeByRankTwoItensAsyncItensSucceeds")]
        public virtual async Task RangeByRankTwoItensAsyncItensSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByRankAsync(2, 3);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 2);
            AssertEquals(rangedItens.ToListAsync().Result.FirstOrDefault(), thirdItem);
            AssertEquals(rangedItens.ToListAsync().Result.LastOrDefault(), fourthItem);
        }

        [Fact(DisplayName = "RangeByRankMoreIndexesOnlyOneItensSucceeds")]
        public virtual async Task RangeByRankMoreIndexesOnlyOneItensSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.GetRangeByRankAsync(3, 6);

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 1);
            AssertEquals(rangedItens.ToListAsync().Result.FirstOrDefault(), fourthItem);
        }

        [Fact(DisplayName = "GetListEnumerableSucceeds")]
        public virtual async Task GetListEnumerableSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.AsEnumerableAsync();

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 4);
            AssertEquals(rangedItens.ToListAsync().Result.FirstOrDefault(), firstItem);
            AssertEquals(rangedItens.ToListAsync().Result.LastOrDefault(), fourthItem);
        }

        [Fact(DisplayName = "GetListEnumerableWithScoreSucceeds")]
        public virtual async Task GetListEnumerableWithScoreSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Act
            var rangedItens = sortedSet.AsEnumerableWithScoreAsync();

            // Assert
            AssertEquals(rangedItens.CountAsync().Result, 4);
            AssertEquals(rangedItens.ToListAsync().Result.FirstOrDefault(), new KeyValuePair<double, T>(0.01, firstItem));
            AssertEquals(rangedItens.ToListAsync().Result.LastOrDefault(), new KeyValuePair<double, T>(0.04, fourthItem));
        }

        [Fact(DisplayName = "DequeueEmptyReturnsDefault")]
        public virtual async Task DequeueEmptyReturnsDefault()
        {
            // Arrange
            var sortedSet = Create();

            // Act
            var actual = await sortedSet.RemoveMinOrDefaultAsync();

            // Assert
            AssertIsDefault(actual);
            AssertEquals(await sortedSet.GetLengthAsync(), 0);
        }

        [Fact(DisplayName = "DequeueMinExistingItemSucceeds")]
        public virtual async Task DequeueMinExistingItemPerMinimumSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            // Act
            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 4);
            AssertEquals(await sortedSet.RemoveMinOrDefaultAsync(), firstItem);
            AssertEquals(await sortedSet.RemoveMinOrDefaultAsync(), secondItem);
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
        }

        [Fact(DisplayName = "DequeueMaxExistingItemSucceeds")]
        public virtual async Task DequeueMaxExistingItemPerMinimumSucceeds()
        {
            // Arrange
            var sortedSet = Create();
            var firstItem = Fixture.Create<T>();
            var secondItem = Fixture.Create<T>();
            var thirdItem = Fixture.Create<T>();
            var fourthItem = Fixture.Create<T>();

            // Act
            await sortedSet.AddAsync(secondItem, 0.02);
            await sortedSet.AddAsync(firstItem, 0.01);
            await sortedSet.AddAsync(fourthItem, 0.04);
            await sortedSet.AddAsync(thirdItem, 0.03);

            // Assert
            AssertEquals(await sortedSet.GetLengthAsync(), 4);
            AssertEquals(await sortedSet.RemoveMaxOrDefaultAsync(), fourthItem);
            AssertEquals(await sortedSet.RemoveMaxOrDefaultAsync(), thirdItem);
            AssertEquals(await sortedSet.GetLengthAsync(), 2);
        }
    }
}