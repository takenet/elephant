using Take.Elephant.Sql;
using Xunit;
using NSubstitute;
using System.Data.Common;
using System.Threading.Tasks;

namespace Take.Elephant.Tests.Sql
{
    public class DbConnectionDecoratorFacts : FactsBase
    {
        private readonly DbConnection _dbConnection;

        public DbConnectionDecoratorFacts()
        {
            _dbConnection = Substitute.For<DbConnection>();
        }

        [Fact]
        public void DisposeOnDecoratorShouldDisposeDbConnection()
        {
            // Arrange
            var decorator = new ExpirationDbConnectionDecorator(_dbConnection);

            // Act
            decorator.Dispose();

            // Assert
            _dbConnection.Received(1).Dispose();
        }

        [Fact]
        public async Task DisposeAsyncOnDecoratorShouldDisposeAsyncDbConnection()
        {
            // Arrange
            var decorator = new ExpirationDbConnectionDecorator(_dbConnection);

            // Act
            await decorator.DisposeAsync();

            // Assert
            await _dbConnection.Received(1).DisposeAsync();
        }
    }
}
