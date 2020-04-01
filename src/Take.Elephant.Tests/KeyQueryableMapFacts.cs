using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class KeyQueryableMapFacts<TKey, TValue> : FactsBase
    {
        public abstract Task<IKeyQueryableMap<TKey, TValue>> CreateAsync(params KeyValuePair<TKey, TValue>[] values);

        public abstract Expression<Func<TValue, bool>> CreateFilter(TValue value);

        public virtual TKey CreateKey()
        {
            return Fixture.Create<TKey>();
        }

        public virtual TValue CreateValue(TKey key)
        {
            return Fixture.Create<TValue>();
        }

        [Fact(DisplayName = "QueryAllExistingKeysSucceeds")]
        public async Task QueryAllExistingKeysSucceeds()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var value3 = CreateValue(key3);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value1),
                new KeyValuePair<TKey, TValue>(key2, value2),
                new KeyValuePair<TKey, TValue>(key3, value3));
            var skip = 0;
            var take = 5;
            Expression<Func<TKey, TKey>> selectFunc = v => v;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(null, selectFunc, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 3);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);
            Check.That(actualList).Contains(key1);
            Check.That(actualList).Contains(key2);
            Check.That(actualList).Contains(key3);
        }

        [Fact(DisplayName = "QueryExistingKeySucceeds")]
        public async Task QueryExistingKeySucceeds()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var value3 = CreateValue(key3);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value1),
                new KeyValuePair<TKey, TValue>(key2, value2),
                new KeyValuePair<TKey, TValue>(key3, value3));
            var skip = 0;
            var take = 5;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(CreateFilter(value2), null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 1);
            Check.That(actualList).Contains(key2);
        }

        [Fact(DisplayName = "QueryExistingKeysWithSingleValueSucceeds")]
        public async Task QueryExistingKeysWithSingleValueSucceeds()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var value = CreateValue(key1);
            var key4 = CreateKey();
            var anyValue = CreateValue(key4);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value),
                new KeyValuePair<TKey, TValue>(key2, value),
                new KeyValuePair<TKey, TValue>(key3, value),
                new KeyValuePair<TKey, TValue>(key4, anyValue));
            
            var skip = 0;
            var take = 5;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(CreateFilter(value), null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 3);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);
            Check.That(actualList).Contains(key1);
            Check.That(actualList).Contains(key2);
            Check.That(actualList).Contains(key3);
        }

        [Fact(DisplayName = "QueryNonExistingKeyReturnsNone")]
        public async Task QueryNonExistingKeyReturnsNone()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var value3 = CreateValue(key3);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value1),
                new KeyValuePair<TKey, TValue>(key3, value3));
            var skip = 0;
            var take = 5;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(CreateFilter(value2), null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 0);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 0);
        }     

        [Fact(DisplayName = "QueryExistingKeysWithTakeLimitSucceeds")]
        public async Task QueryExistingKeysWithTakeLimitSucceeds()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var key4 = CreateKey();
            var key5 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var value3 = CreateValue(key3);
            var value4 = CreateValue(key3);
            var value5 = CreateValue(key3);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value1),
                new KeyValuePair<TKey, TValue>(key2, value2),
                new KeyValuePair<TKey, TValue>(key3, value3),
                new KeyValuePair<TKey, TValue>(key4, value4),
                new KeyValuePair<TKey, TValue>(key5, value5));
            var skip = 0;
            var take = 3;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(null, null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 5);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 3);
        }

        [Fact(DisplayName = "QueryExistingKeysWithTakeAndSkipLimitSucceeds")]
        public async Task QueryExistingKeysWithTakeAndSkipLimitSucceeds()
        {
            // Arrange
            var key1 = CreateKey();
            var key2 = CreateKey();
            var key3 = CreateKey();
            var key4 = CreateKey();
            var key5 = CreateKey();
            var value1 = CreateValue(key1);
            var value2 = CreateValue(key2);
            var value3 = CreateValue(key3);
            var value4 = CreateValue(key3);
            var value5 = CreateValue(key3);
            var map = await CreateAsync(
                new KeyValuePair<TKey, TValue>(key1, value1),
                new KeyValuePair<TKey, TValue>(key2, value2),
                new KeyValuePair<TKey, TValue>(key3, value3),
                new KeyValuePair<TKey, TValue>(key4, value4),
                new KeyValuePair<TKey, TValue>(key5, value5));
            var skip = 3;
            var take = 3;

            // Act
            var actual = await map.QueryForKeysAsync<TKey>(null, null, skip, take, CancellationToken.None);

            // Assert
            AssertEquals(actual.Total, 5);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 2);
        }


    }
}
