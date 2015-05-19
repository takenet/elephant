using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class QueryableStorageFacts<T> : FactsBase
    {
        public abstract Task<IQueryableStorage<T>> CreateAsync(params T[] values);

        public abstract Expression<Func<T, bool>> CreateFilter(T value);

        public virtual T CreateValue()
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = "QueryAllValuesSucceeds")]
        public virtual async Task QueryAllValuesSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value3);

            // Act
            var actual = await storage.QueryAsync<T>(null, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);
            Check.That(actualList).Contains(value1);
            Check.That(actualList).Contains(value2);
            Check.That(actualList).Contains(value3);
            AssertEquals(actual.Total, 3);
        }

        [Fact(DisplayName = "QueryExistingValueSucceeds")]
        public virtual async Task QueryExistingValueSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value3);
            
            // Act
            var actual = await storage.QueryAsync<T>(CreateFilter(value2), null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 1);
            Check.That(actualList).Contains(value2);
            AssertEquals(actual.Total, 1);
        }

        [Fact(DisplayName = "QueryNonExistingValueReturnsNone")]
        public virtual async Task QueryNonExistingValueReturnsNone()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value3);

            // Act
            var actual = await storage.QueryAsync<T>(CreateFilter(value2), null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 0);
            AssertEquals(actual.Total, 0);
        }

        [Fact(DisplayName = "QueryExistingValueWithTakeLimitSucceeds")]
        public virtual async Task QueryExistingValueWithTakeLimitSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var value4 = CreateValue();
            var value5 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 3;
            var storage = await CreateAsync(value1, value2, value3, value4, value5);

            // Act
            var actual = await storage.QueryAsync<T>(null, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, take);
            AssertEquals(actual.Total, 5);
        }

        [Fact(DisplayName = "QueryExistingValueWithTakeAndSkipLimitSucceeds")]
        public virtual async Task QueryExistingValueWithTakeAndSkipLimitSucceeds()
        {
            // Arrange            
            var value1 = CreateValue();
            var value2 = CreateValue();
            var value3 = CreateValue();
            var value4 = CreateValue();
            var value5 = CreateValue();
            var cancellationToken = CancellationToken.None;
            var skip = 3;
            var take = 3;
            var storage = await CreateAsync(value1, value2, value3, value4, value5);

            // Act
            var actual = await storage.QueryAsync<T>(null, null, skip, take, cancellationToken);

            // Assert            
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 2);
            AssertEquals(actual.Total, 5);
        }
    }
}