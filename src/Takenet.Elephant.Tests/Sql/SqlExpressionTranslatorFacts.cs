using System;
using System.Linq.Expressions;
using Takenet.Elephant.Sql;
using Xunit;
using System.Data;
using System.Data.Common;

namespace Takenet.Elephant.Tests.Sql
{
    [Trait("Category", nameof(Memory))]
    public class SqlExpressionTranslatorFacts : FactsBase
    {
        public SqlExpressionTranslatorFacts()
        {
            DatabaseDriver = new FakeDatabaseDriver();
        }

        public IDatabaseDriver DatabaseDriver { get; }


        public SqlExpressionTranslator GetTarget()
        {
            return new SqlExpressionTranslator(DatabaseDriver);
        }


        [Fact]
        public void SingleEqualsConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] = 'abcd')");
        }

        [Fact]
        public void SingleEqualsExplicitBooleanConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.BooleanProperty == false || i.BooleanProperty == true;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);
            
            // Assert
            AssertEquals(actual, "(([BooleanProperty] = 0) OR ([BooleanProperty] = 1))");
        }

        [Fact(Skip = "Not supported yet")]
        public void SingleEqualsBooleanConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.BooleanProperty;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([BooleanProperty] = 1)");
        }

        [Fact]
        public void SingleNotEqualsConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty != "abcd";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] <> 'abcd')");
        }

        [Fact]
        public void SingleNullConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty == null;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] IS NULL)");
        }

        [Fact]
        public void SingleNotNullConstantClauseShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty != null;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] IS NOT NULL)");
        }

        [Fact]
        public void SingleEqualsMemberClauseShouldCreateSql()
        {
            // Arrange            
            var item = new Item
            {
                StringProperty = "abcd"
            };

            Expression<Func<Item, bool>> expression = i => i.StringProperty == item.StringProperty;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] = 'abcd')");
        }

        [Fact]
        public void SingleEqualsExternalMemberClauseShouldCreateSql()
        {
            // Arrange            
            var item = new TestItem
            {
                Value1 = "abcd",
                Value2 = 2
            };

            Expression<Func<Item, bool>> expression = i => i.StringProperty == item.Value1;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] = 'abcd')");
        }

        [Fact(Skip ="Not supported yet")]
        public void SingleEqualsNullMemberClauseShouldCreateSql()
        {
            // Arrange            
            var item = new Item
            {
                StringProperty = null
            };

            Expression<Func<Item, bool>> expression = i => i.StringProperty == item.StringProperty;
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "([StringProperty] IS NULL)");
        }

        [Fact]
        public void MultipleConstantsAndClausesShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd" && i.IntegerProperty == 2 && i.RandomProperty == "random value";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "((([StringProperty] = 'abcd') AND ([IntegerProperty] = 2)) AND ([RandomProperty] = 'random value'))");
        }


        [Fact]
        public void MultipleConstantsOrClausesShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd" || i.IntegerProperty == 2 || i.RandomProperty == "random value";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "((([StringProperty] = 'abcd') OR ([IntegerProperty] = 2)) OR ([RandomProperty] = 'random value'))");
        }

        [Fact]
        public void MultipleConstantsAndOrClausesShouldCreateSql()
        {
            // Arrange            
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd" && i.IntegerProperty == 2 || i.RandomProperty == "random value";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "((([StringProperty] = 'abcd') AND ([IntegerProperty] = 2)) OR ([RandomProperty] = 'random value'))");
        }

        [Fact]
        public void MultipleConstantAndMemberAccessAndClausesShouldCreateSql()
        {
            // Arrange        
            var item = new Item
            {
                StringProperty = "abcd",
                IntegerProperty = 2
            };
            Expression<Func<Item, bool>> expression = i => i.StringProperty == item.StringProperty && i.IntegerProperty == item.IntegerProperty && i.RandomProperty == "random value";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual, "((([StringProperty] = 'abcd') AND ([IntegerProperty] = 2)) AND ([RandomProperty] = 'random value'))");
        }


        class TestItem
        {
            public string Value1 { get; set; }

            public int Value2 { get; set; }

        }
    }

    internal class FakeDatabaseDriver : IDatabaseDriver
    {
        public readonly SqlDatabaseDriver _sqlDatabaseDriver;

        public FakeDatabaseDriver()
        {
            _sqlDatabaseDriver = new SqlDatabaseDriver();
        }

        public string DefaultSchema => _sqlDatabaseDriver.DefaultSchema;

        public TimeSpan Timeout => _sqlDatabaseDriver.Timeout;

        public DbConnection CreateConnection(string connectionString)
        {
            throw new NotSupportedException();
        }

        public DbParameter CreateParameter(string parameterName, object value)
        {
            return _sqlDatabaseDriver.CreateParameter(parameterName, value);
        }

        public string GetSqlStatementTemplate(SqlStatement sqlStatement)
        {
            return _sqlDatabaseDriver.GetSqlStatementTemplate(sqlStatement);
        }

        public string GetSqlTypeName(DbType dbType)
        {
            return _sqlDatabaseDriver.GetSqlTypeName(dbType);
        }

        public string ParseIdentifier(string identifier)
        {
            return _sqlDatabaseDriver.ParseIdentifier(identifier);
        }

        public string ParseParameterName(string parameterName)
        {
            return _sqlDatabaseDriver.ParseParameterName(parameterName);
        }
    }
}
