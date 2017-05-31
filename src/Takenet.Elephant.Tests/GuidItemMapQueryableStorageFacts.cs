using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Takenet.Elephant.Tests
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
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.StartsWith(queryValue), null, 0, 1, cancellationToken);

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
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.EndsWith(queryValue), null, 0, 1, cancellationToken);

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
            var actual = await storage.QueryAsync<Item>(i => i.StringProperty.Contains(queryValue), null, 0, 1, cancellationToken);

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
            var actual = await storage.QueryAsync<Item>(i => i.UriProperty.ToString().Contains(queryValue), null, 0, 1, cancellationToken);

            // Assert            
            AssertEquals(actual.Total, 1);
            var actualList = await actual.ToListAsync();
            AssertEquals(actualList[0], value2);
        }
    }
}