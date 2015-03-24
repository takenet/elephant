using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NFluent;
using Ploeh.AutoFixture;
using Xunit;

namespace Takenet.SimplePersistence.Tests
{
    public abstract class SetFacts<T>
    {
        private readonly Fixture _fixture;

        protected SetFacts()
        {
            _fixture = new Fixture();
        }

        public abstract ISet<T> Create();

        [Fact(DisplayName = "AddNewItemSucceeds")]
        public async Task AddNewItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = _fixture.Create<T>();

            // Act
            await set.AddAsync(item);

            // Assert
            Check.That(await set.ContainsAsync(item)).IsTrue();
        }

        [Fact(DisplayName = "AddExistingItemSucceeds")]
        public async Task AddExistingItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = _fixture.Create<T>();
            await set.AddAsync(item);

            // Act
            await set.AddAsync(item);

            // Assert
            Check.That(await set.ContainsAsync(item)).IsTrue();
        }

        [Fact(DisplayName = "TryRemoveExistingItemSucceeds")]
        public async Task TryRemoveExistingItemSucceeds()
        {
            // Arrange
            var set = Create();
            var item = _fixture.Create<T>();
            await set.AddAsync(item);

            // Act
            var result = await set.TryRemoveAsync(item);

            // Assert
            Check.That(result).IsTrue();
            Check.That(await set.ContainsAsync(item)).IsFalse();
        }

        [Fact(DisplayName = "TryRemoveNonExistingItemFails")]
        public async Task TryRemoveNonExistingItemFails()
        {
            // Arrange
            var set = Create();
            var item = _fixture.Create<T>();

            // Act
            var result = await set.TryRemoveAsync(item);

            // Assert
            Check.That(result).IsFalse();            
        }
    }
}
