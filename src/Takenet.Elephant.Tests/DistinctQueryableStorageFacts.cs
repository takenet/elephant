using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class DistinctQueryableStorageFacts<T> : FactsBase
    {
        public abstract Task<IDistinctQueryableStorage<T>> CreateAsync(params T[] values);

        public abstract Expression<Func<T, bool>> CreateFilter(T value);

        public virtual T CreateValue()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = nameof(QueryDistinctValuesSucceeds))]
        public virtual async Task QueryDistinctValuesSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value2, value3, value3, value3);
            Expression<Func<T, T>> selectFunc = v => v;

            // Act
            var actual = await storage.QueryAsync<T>(null, selectFunc, true, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);
            Check.That(actualList).Contains(value1);
            Check.That(actualList).Contains(value2);
            Check.That(actualList).Contains(value3);
            AssertEquals(actual.Total, 3);
        }

        [Fact(DisplayName = nameof(QueryDistinctFilteredValuesSucceeds))]
        public virtual async Task QueryDistinctFilteredValuesSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value2, value3, value3, value3);
            var whereFunc = CreateFilter(value3);
            Expression<Func<T, T>> selectFunc = v => v;
            

            // Act
            var actual = await storage.QueryAsync<T>(whereFunc, selectFunc, true, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 1);
            Check.That(actualList).Contains(value3);
            AssertEquals(actual.Total, 1);
        }


        [Fact(DisplayName = nameof(QueryNonDistinctValuesSucceeds))]
        public virtual async Task QueryNonDistinctValuesSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value2, value3, value3, value3);
            Expression<Func<T, T>> selectFunc = v => v;

            // Act
            var actual = await storage.QueryAsync<T>(null, selectFunc, false, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 6);
            Check.That(actualList).Contains(value1);
            Check.That(actualList).Contains(value2);
            Check.That(actualList).Contains(value3);
            AssertEquals(actual.Total, 6);
        }

        [Fact(DisplayName = nameof(QueryNonDistinctFilteredValuesSucceeds))]
        public virtual async Task QueryNonDistinctFilteredValuesSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value2, value3, value3, value3);
            var whereFunc = CreateFilter(value3);
            Expression<Func<T, T>> selectFunc = v => v;

            // Act
            var actual = await storage.QueryAsync<T>(whereFunc, selectFunc, false, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);            
            Check.That(actualList).Contains(value3);
            AssertEquals(actual.Total, 3);
        }
    }
}