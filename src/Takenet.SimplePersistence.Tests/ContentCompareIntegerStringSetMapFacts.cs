using System.Threading.Tasks;
using NFluent;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    /// <summary>
    /// Compares the set content instead of the instance
    /// </summary>
    public abstract class ContentCompareIntegerStringSetMapFacts : IntegerStringSetMapFacts
    {
        [Fact(DisplayName = "AddExistingKeyAndValueFails")]
        public async override Task AddExistingKeyAndValueFails()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, false);

            // Assert
            Check.That(actual).IsFalse();
            Check.That(await (await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ToListAsync()).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "AddNewKeyAndValueSucceeds")]
        public async override Task AddNewKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, value, false);

            // Assert
            Check.That(actual).IsTrue();
            Check.That(await (await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ToListAsync()).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "GetExistingKeyReturnsValue")]
        public async override Task GetExistingKeyReturnsValue()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);

            // Act
            var actual = await map.GetValueOrDefaultAsync(key);

            // Assert
            var items = await (await actual.AsEnumerableAsync()).ToListAsync();
            Check.That(items).ContainsExactly(await value.AsEnumerableAsync());
        }

        [Fact(DisplayName = "OverwriteExistingKeyAndValueSucceeds")]
        public async override Task OverwriteExistingKeyAndValueSucceeds()
        {
            // Arrange
            var map = Create();
            var key = CreateKey();
            var value = CreateValue(key);
            await map.TryAddAsync(key, value, false);
            var newValue = CreateValue(key);

            // Act
            var actual = await map.TryAddAsync(key, newValue, true);

            // Assert            
            Check.That(actual).IsTrue();
            Check.That(await (await (await map.GetValueOrDefaultAsync(key)).AsEnumerableAsync()).ToListAsync()).ContainsExactly(await newValue.AsEnumerableAsync());
        }
    }
}