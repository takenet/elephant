using System;
using System.Linq.Expressions;
using Take.Elephant.Sql;
using Xunit;
using System.Data;
using System.Data.Common;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
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
            return new SqlExpressionTranslator(DatabaseDriver, DbTypeMapper.Default);
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
            AssertEquals(actual.Where, "([StringProperty] = @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
        }

        [Fact]
        public void SingleEqualsConstantWithComplexTypeClauseShouldCreateSql()
        {
            // Arrange
            Expression<Func<TestItem, bool>> expression = i => i.Value3.ToString() == "XYZ";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "([Value3] = @Value3)");
            AssertEquals(actual.FilterValues["Value3"], "XYZ");
        }

        [Fact]
        public void SingleContainsConstantClauseShouldCreateSql()
        {
            // Arrange
            Expression<Func<Item, bool>> expression = i => i.StringProperty.Contains("abcd");
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "([StringProperty] LIKE @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "%abcd%");
        }

        [Fact]
        public void SingleStartsWithConstantClauseShouldCreateSql()
        {
            // Arrange
            Expression<Func<Item, bool>> expression = i => i.StringProperty.StartsWith("abcd");
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "([StringProperty] LIKE @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd%");
        }

        [Fact]
        public void SingleEndsWithConstantClauseShouldCreateSql()
        {
            // Arrange
            Expression<Func<Item, bool>> expression = i => i.StringProperty.EndsWith("abcd");
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "([StringProperty] LIKE @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "%abcd");
        }

        [Fact]
        public void SingleEqualsWithSqlInjectionShouldBeHandled()
        {
            // Arrange
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd'); DROP TABLE MyTable; --";
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "([StringProperty] = @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd'); DROP TABLE MyTable; --");
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
            AssertEquals(actual.Where, "(([BooleanProperty] = @BooleanProperty) OR ([BooleanProperty] = @BooleanProperty$1))");
            AssertEquals(actual.FilterValues["BooleanProperty"], false);
            AssertEquals(actual.FilterValues["BooleanProperty$1"], true);
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
            AssertEquals(actual.Where, "([BooleanProperty] = @Param0)");
            AssertEquals(actual.FilterValues["Param0"], true);
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
            AssertEquals(actual.Where, "([StringProperty] <> @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
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
            AssertEquals(actual.Where, "([StringProperty] IS NULL)");
            AssertEquals(actual.FilterValues.Count, 0);
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
            AssertEquals(actual.Where, "([StringProperty] IS NOT NULL)");
            AssertEquals(actual.FilterValues.Count, 0);
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
            AssertEquals(actual.Where, "([StringProperty] = @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
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
            AssertEquals(actual.Where, "([StringProperty] = @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
        }

        [Fact(Skip = "Not supported yet")]
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
            AssertEquals(actual.Where, "([StringProperty] = @StringProperty)");
            AssertEquals(actual.FilterValues["StringProperty"], null);
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
            AssertEquals(actual.Where, "((([StringProperty] = @StringProperty) AND ([IntegerProperty] = @IntegerProperty)) AND ([RandomProperty] = @RandomProperty))");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
            AssertEquals(actual.FilterValues["IntegerProperty"], 2);
            AssertEquals(actual.FilterValues["RandomProperty"], "random value");
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
            AssertEquals(actual.Where, "((([StringProperty] = @StringProperty) OR ([IntegerProperty] = @IntegerProperty)) OR ([RandomProperty] = @RandomProperty))");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
            AssertEquals(actual.FilterValues["IntegerProperty"], 2);
            AssertEquals(actual.FilterValues["RandomProperty"], "random value");
        }

        [Fact]
        public void MultipleConstantsAndOrClausesShouldCreateSql()
        {
            // Arrange
            Expression<Func<Item, bool>> expression = i => i.StringProperty == "abcd" && i.IntegerProperty == 2 || i.RandomProperty.Contains("random value");
            var target = GetTarget();

            // Act
            var actual = target.GetStatement(expression);

            // Assert
            AssertEquals(actual.Where, "((([StringProperty] = @StringProperty) AND ([IntegerProperty] = @IntegerProperty)) OR ([RandomProperty] LIKE @RandomProperty))");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
            AssertEquals(actual.FilterValues["IntegerProperty"], 2);
            AssertEquals(actual.FilterValues["RandomProperty"], "%random value%");
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
            AssertEquals(actual.Where, "((([StringProperty] = @StringProperty) AND ([IntegerProperty] = @IntegerProperty)) AND ([RandomProperty] = @RandomProperty))");
            AssertEquals(actual.FilterValues["StringProperty"], "abcd");
            AssertEquals(actual.FilterValues["IntegerProperty"], 2);
            AssertEquals(actual.FilterValues["RandomProperty"], "random value");
        }

        private class TestItem
        {
            public string Value1 { get; set; }

            public int Value2 { get; set; }

            public TestItem Value3 { get; set; }

            public override string ToString() => $"{Value1}:{Value2}:{Value3}";
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

        public DbParameter CreateParameter(string parameterName, object value, SqlType sqlType)
        {
            return _sqlDatabaseDriver.CreateParameter(parameterName, value, sqlType);
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