using System;
using System.Linq.Expressions;
using Take.Elephant.Sql;
using Xunit;
using System.Data;
using Take.Elephant.Sql.Mapping;
using System.Reflection;
using System.Linq;
using Shouldly;

namespace Take.Elephant.Tests.Sql
{
    public class SqlConditionsFacts : FactsBase
    {
        public SqlConditionsFacts()
        {
            DatabaseDriver = new FakeDatabaseDriver();
        }

        public IDatabaseDriver DatabaseDriver { get; }

        public SqlWhereStatement GetTarget(Expression expression)
        {
            var sqlExpressionTranslator = new SqlExpressionTranslator(DatabaseDriver, DbTypeMapper.Default);
            return sqlExpressionTranslator.GetStatement(expression);
        }

        public ITable GetTable()
        {
            Func<PropertyInfo, bool> PropertyFilter = (f) => !f.PropertyType.IsArray;

            ITable FakeTable = TableBuilder
                .WithName("FakeTable")
                .WithKeyColumnsNames(nameof(FakeDocumentCondition.Id))
                .WithColumnsFromTypeProperties<FakeDocumentCondition>(PropertyFilter)
                .WithColumn(nameof(FakeDocumentCondition.PersonalField), new SqlType(DbType.String, 500))
                .Build();

            return FakeTable;
        }

        [Fact]
        public void MultipleConditionsSqlWhereStatement()
        {
            var param0 = 999;
            var param1 = "text1";
            var param2 = "text2";
            var param3 = "abcde";
            var param4 = new DateTimeOffset(1980, 07, 15, 12, 10, 07, TimeSpan.Zero);
            var param5 = 12.99m;
            var param6 = 85.75;
            var param7 = true;
            var param8 = Guid.NewGuid();
            DateTime? param9 = DateTime.Now;

            // Arrange
            Expression<Func<FakeDocumentCondition, bool>> expression = i =>
                i.Id == param0 &&
                i.Name == param1 &&
                (i.PersonalField == param2 || i.PersonalField == param3) &&
                i.DateProperty == param4 &&
                (i.DecimalProperty == param5 || i.FloatProperty == param6) &&
                i.BooleanProperty == param7 &&
                i.GuidProperty == param8 &&
                (i.DateTimeProperty == null || i.DateTimeProperty == param9 || i.DateTimeProperty != null) &&
                i.StringProperty == "not null" &&
                i.StringProperty != "not null";

            var target = GetTarget(expression);

            // Act
            var paramList = target.FilterValues?.ToDbParameters(DatabaseDriver, GetTable());
            var actual = Enumerable.ToList(paramList);

            // Assert
            AssertEquals(actual.Count, target.FilterValues.Count);

            foreach (var item in target.FilterValues)
            {
                var param = actual.FirstOrDefault(p => p.ParameterName == $"@{item.Key}");
                Assert.NotNull(param);
                AssertEquals(param.Value, item.Value);
            }

            target.Where.ShouldNotBeNullOrEmpty();
            target.Where.ShouldContain("[Id] = @Id");
            target.Where.ShouldContain("[Name] = @Name");
            target.Where.ShouldContain("[PersonalField] = @PersonalField");
            target.Where.ShouldContain("[PersonalField] = @PersonalField__");
            target.Where.ShouldContain("[DateProperty] = @DateProperty");
            target.Where.ShouldContain("[DecimalProperty] = @DecimalProperty");
            target.Where.ShouldContain("[FloatProperty] = @FloatProperty");
            target.Where.ShouldContain("[BooleanProperty] = @BooleanProperty");
            target.Where.ShouldContain("[GuidProperty] = @GuidProperty");
            target.Where.ShouldContain("[DateTimeProperty] IS NULL");
            target.Where.ShouldContain("[DateTimeProperty] = @DateTimeProperty");
            target.Where.ShouldContain("[StringProperty] = @StringProperty");
            target.Where.ShouldContain("[StringProperty] <> @StringProperty__");
            target.Where.ShouldContain("[DateTimeProperty] IS NOT NULL");
        }
    }

    internal class FakeDocumentCondition : Item
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string PersonalField { get; set; }
        public DateTime? DateTimeProperty { get; set; }
        public string StringProperty { get; set; }
    }
}