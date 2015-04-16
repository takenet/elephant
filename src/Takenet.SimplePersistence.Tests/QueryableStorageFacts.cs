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
        }
    }
}