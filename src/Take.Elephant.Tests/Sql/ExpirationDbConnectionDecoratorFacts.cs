using Take.Elephant.Sql;
using Xunit;
using NSubstitute;
using System.Data.Common;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.Sql
{
    public class ExpirationDbConnectionDecoratorFacts : FactsBase
    {
        private readonly DbConnection _dbConnection;

        public ExpirationDbConnectionDecoratorFacts()
        {
            _dbConnection = Substitute.For<DbConnection>();
        }

        [Fact]
        public void DisposeOnDecoratorShouldDisposeDbConnection()
        {
            // Arrange
            var decorator = new ExpirableKeySqlMap<int, int>.ExpirationDbConnectionDecorator(_dbConnection);

            // Act
            decorator.Dispose();

            // Assert
            _dbConnection.Received(1).Dispose();
        }

        [Fact]
        public async Task DisposeAsyncOnDecoratorShouldDisposeAsyncDbConnection()
        {
            // Arrange
            var decorator = new ExpirableKeySqlMap<int, int>.ExpirationDbConnectionDecorator(_dbConnection);

            // Act
            await decorator.DisposeAsync();

            // Assert
            await _dbConnection.Received(1).DisposeAsync();
        }
    }
}
