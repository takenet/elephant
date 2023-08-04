using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Take.Elephant.Tests
{
    public abstract class GuidItemMapQueryableStorageFacts : MapQueryableStorageFacts<Guid, Item>
    {
        public override Expression<Func<Item, bool>> CreateFilter(Item value)
        {
            var randomGuid = Guid.NewGuid();
            var randomString1 = Guid.NewGuid().ToString();
            var randomString2 = Guid.NewGuid().ToString();
            return
                i =>
                    i.GuidProperty != randomGuid &&
                    i.GuidProperty.Equals(value.GuidProperty) &&
                    i.IntegerProperty.Equals(value.IntegerProperty) &&
                    i.IntegerProperty != -1291387 &&
                    !i.StringProperty.StartsWith(randomString1) &&
                    !i.StringProperty.EndsWith(randomString2) &&
                    i.StringProperty.Equals(value.StringProperty) &&
                    i.StringProperty.StartsWith(value.StringProperty) &&
                    i.StringProperty.EndsWith(value.StringProperty) &&
                    i.StringProperty != "ignore me";
        }

        [Fact(DisplayName = "QueryExistingValueWithEqualsOperatorSucceeds")]
        public virtual async Task QueryExistingValueWithEqualsOperatorSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "abcdefgh1234567890";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.Equals(queryValue), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value1);
        }

        [Fact(DisplayName = nameof(QueryExistingValueWithEqualsNotNullSucceeds))]
        public virtual async Task QueryExistingValueWithEqualsNotNullSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = null;
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "abcdefgh1234567890";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty != null, null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 2);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 2);
            AssertIsTrue(actualList.Contains(value1));
            AssertIsTrue(actualList.Contains(value3));
        }

        [Fact(DisplayName = nameof(QueryExistingValueWithEqualsNotNullAndMultipleFiltersSucceeds))]
        public virtual async Task QueryExistingValueWithEqualsNotNullAndMultipleFiltersSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            value1.RandomProperty = "1234567890";
            value1.BooleanNullProperty = null;
            var value2 = CreateValue();
            value2.StringProperty = null;
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);

            Expression<Func<Item, bool>> filter =
                i => i.BooleanNullProperty == null &&
                i.RandomProperty == "1234567890";

            // Act
            var actual = await storage.QueryAsync<Item>(filter, null, 0, 5, cancellationToken);

            // Assert
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList.Count, 1);
            AssertIsTrue(actualList.Contains(value1));
        }

        [Fact(DisplayName = nameof(QueryExistingValueWithEqualsNullSucceeds))]
        public virtual async Task QueryExistingValueWithEqualsNullSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = null;
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "abcdefgh1234567890";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty == null, null, 0, 1, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value2);
        }

        [Fact(DisplayName = "QueryExistingValueWithContainsAndOperatorsSucceeds")]
        public virtual async Task QueryExistingValueWithContainsAndOperatorsSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            value1.IntegerProperty = 101;
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            value2.IntegerProperty = 102;
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            value3.IntegerProperty = 103;
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue1 = "1234567890";
            var queryValue2 = 103;

            // Act
            var actual = await storage.QueryAsync<Item>(
                i => i.StringProperty.Contains(queryValue1) && i.IntegerProperty.Equals(queryValue2), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value3);
        }

        [Fact(DisplayName = "QueryExistingValueWithEqualsOrOperatorsSucceeds")]
        public virtual async Task QueryExistingValueWithEqualsOrOperatorsSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            value1.IntegerProperty = 101;
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            value2.IntegerProperty = 102;
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            value3.IntegerProperty = 103;
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue1 = "xyz0987654321";
            var queryValue2 = 104;

            // Act
            var actual = await storage.QueryAsync<Item>(
                i => i.StringProperty.Contains(queryValue1) || i.IntegerProperty.Equals(queryValue2), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value2);
        }

        [Fact(DisplayName = "QueryExistingValueWithStartsWithSucceeds")]
        public virtual async Task QueryExistingValueWithStartsWithSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "abcd";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.StartsWith(queryValue), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value1);
        }

        [Fact(DisplayName = "QueryExistingValueWithEndsWithSucceeds")]
        public virtual async Task QueryExistingValueWithEndsWithSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "abcd";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.EndsWith(queryValue), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value3);
        }

        [Fact(DisplayName = "QueryExistingValueWithContainsSucceeds")]
        public virtual async Task QueryExistingValueWithContainsSucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.StringProperty = "abcdefgh1234567890";
            var value2 = CreateValue();
            value2.StringProperty = "xyz0987654321";
            var value3 = CreateValue();
            value3.StringProperty = "1234567890xyzabcd";
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "0987";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.Contains(queryValue), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value2);
        }

        [Fact(DisplayName = "QueryExistingValueWithContainsWithNonStringPropertySucceeds")]
        public virtual async Task QueryExistingValueWithContainsWithNonStringPropertySucceeds()
        {
            // Arrange
            var value1 = CreateValue();
            value1.UriProperty = new Uri("http://abcdefgh1234567890");
            var value2 = CreateValue();
            value2.UriProperty = new Uri("http://xyz0987654321");
            var value3 = CreateValue();
            value3.UriProperty = new Uri("http://1234567890xyzabcd");
            var cancellationToken = CancellationToken.None;
            var storage = await CreateAsync(value1, value2, value3);
            var queryValue = "0987";

            // Act
            var actual = await storage.QueryAsync<Item>(i => i.UriProperty.ToString().Contains(queryValue), null, 0, 5, cancellationToken);

            // Assert
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value2);
        }
    }
}