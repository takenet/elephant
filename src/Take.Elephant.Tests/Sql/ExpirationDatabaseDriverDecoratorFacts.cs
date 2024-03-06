using System;
using System.Threading.Tasks;
using NSubstitute;
using Take.Elephant.Sql;
using Xunit;

namespace Take.Elephant.Tests.Sql
{
    public class ExpirationDatabaseDriverDecoratorFacts : FactsBase
    {
        private readonly IDatabaseDriver _databaseDriver;


        public ExpirationDatabaseDriverDecoratorFacts()
        {
            _databaseDriver = Substitute.For<IDatabaseDriver>();
        }

        [Theory]
        [InlineData(SqlStatement.Select)]
        [InlineData(SqlStatement.SelectCount)]
        [InlineData(SqlStatement.SelectTop1)]
        [InlineData(SqlStatement.SelectSkipTake)]
        [InlineData(SqlStatement.SelectDistinct)]
        [InlineData(SqlStatement.SelectCountDistinct)]
        [InlineData(SqlStatement.SelectDistinctSkipTake)]
        [InlineData(SqlStatement.Exists)]
        public async Task GetSqlStatementTemplateMustBeIdempotent(SqlStatement statement)
        {
            // ExpirationDatabaseDriverDecorator.GetSqlStatementTemplate should inject a parameterized query
            // so that multiple calls to it with the same arguments should always yield the exact same query
            // in order not to thrash the database's plan cache

            // Arrange
            var decorator = new ExpirableKeySqlMap<int, int>.ExpirationDatabaseDriverDecorator(_databaseDriver, "ExpirationDate");

            // Act
            var template1 = decorator.GetSqlStatementTemplate(statement);
            await Task.Delay(TimeSpan.FromSeconds(1));
            var template2 = decorator.GetSqlStatementTemplate(statement);

            // Assert
            Assert.Equal(template2, template1);
        }
    }
}
