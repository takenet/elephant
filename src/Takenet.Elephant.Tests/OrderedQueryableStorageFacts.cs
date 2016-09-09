using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.Elephant.Tests
{
    public abstract class OrderedQueryableStorageFacts<T, TOrderBy> : FactsBase
    {
        public abstract Task<IOrderedQueryableStorage<T>> CreateAsync(params T[] values);

        public abstract Expression<Func<T, bool>> CreateFilter(T value);

        public abstract Expression<Func<T, TOrderBy>> CreateOrderBy();

        public virtual T CreateValue(int order)
        {
            return Fixture.Create<T>();
        }

        [Fact(DisplayName = "QueryAllValuesAscendingSucceeds")]
        public virtual async Task QueryAllValuesAscendingSucceeds()
        {
            // Arrange            
            var value1 = CreateValue(1);
            var value2 = CreateValue(2);
            var value3 = CreateValue(3);
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value3);
            Expression<Func<T, T>> selectFunc = v => v;

            var orderby = CreateOrderBy();

            // Act
            var actual = await storage.QueryAsync<T, TOrderBy>(null, selectFunc, orderby, true, skip, take, cancellationToken);

            // Assert   
            var actualList = await actual.Items.ToListAsync();

            AssertEquals(actualList.Count, 3);
            AssertEquals(actual.Total, 3);
            AssertEquals(actualList[0], value1);
            AssertEquals(actualList[1], value2);
            AssertEquals(actualList[2], value3);                        
        }

        [Fact(DisplayName = "QueryAllValuesDescendingSucceeds")]
        public virtual async Task QueryAllValuesDescendingSucceeds()
        {
            // Arrange            
            var value1 = CreateValue(1);
            var value2 = CreateValue(2);
            var value3 = CreateValue(3);
            var cancellationToken = CancellationToken.None;
            var skip = 0;
            var take = 10;
            var storage = await CreateAsync(value1, value2, value3);
            Expression<Func<T, T>> selectFunc = v => v;

            var orderby = CreateOrderBy();

            // Act
            var actual = await storage.QueryAsync<T, TOrderBy>(null, selectFunc, orderby, false, skip, take, cancellationToken);

            // Assert   
            var actualList = await actual.Items.ToListAsync();

            AssertEquals(actualList.Count, 3);
            AssertEquals(actual.Total, 3);
            AssertEquals(actualList[0], value3);
            AssertEquals(actualList[1], value2);
            AssertEquals(actualList[2], value1);
        }
    }
}