using System;
using System.Linq.Expressions;
using Take.Elephant.Sql;
using Xunit;
using System.Data;
using Take.Elephant.Sql.Mapping;
using System.Reflection;
using System.Linq;
using System.Data.SqlClient;

namespace Take.Elephant.Tests.Sql
{
    public class SqlExtensionsFacts : FactsBase
    {
        public SqlExtensionsFacts()
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
                .WithKeyColumnsNames(nameof(FakeDocument.Id))
                .WithColumnsFromTypeProperties<FakeDocument>(PropertyFilter)
                .WithColumn(nameof(FakeDocument.PersonalField), new SqlType(DbType.String, 500))
                .Build();

            return FakeTable;
        }

        [Fact]
        public void MultipleConditionsClauseShouldCreateRespectiveDbParams()
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

            // Arrange
            Expression<Func<FakeDocument, bool>> expression = i =>
                i.Id == param0 &&
                i.Name == param1 &&
                (i.PersonalField == param2 || i.PersonalField == param3) &&
                i.DateProperty == param4 &&
                (i.DecimalProperty == param5 || i.FloatProperty == param6) &&
                i.BooleanProperty == param7 &&
                i.GuidProperty == param8;

            var target = GetTarget(expression);

            // Act
            var paramList = target.FilterValues?.ToDbParameters(DatabaseDriver, GetTable());
            var actual = Enumerable.ToList(paramList);

            // Assert
            AssertEquals(actual.Count, 9);

            AssertEquals(actual[0].ParameterName, "@Id");
            AssertEquals(actual[0].Value, param0);
            AssertEquals(actual[0].Direction, ParameterDirection.Input);
            AssertEquals(((SqlParameter)actual[0]).SqlDbType, SqlDbType.Int);
            AssertEquals(actual[0].DbType, DbType.Int32);

            //Param string default size (not explicit on create ITable)
            AssertEquals(actual[1].ParameterName, "@Name");
            AssertEquals(actual[1].Value, param1);
            AssertEquals(actual[1].Direction, ParameterDirection.Input);
            AssertEquals(actual[1].DbType, DbType.String);
            AssertEquals(((SqlParameter)actual[1]).SqlDbType, SqlDbType.NVarChar);
            AssertEquals(actual[1].Size, 250);

            //Param compared to field with personalized size (500)
            AssertEquals(actual[2].ParameterName, "@PersonalField");
            AssertEquals(actual[2].Value, param2);
            AssertEquals(actual[2].Direction, ParameterDirection.Input);
            AssertEquals(actual[2].DbType, DbType.String);
            AssertEquals(((SqlParameter)actual[2]).SqlDbType, SqlDbType.NVarChar);
            AssertEquals(actual[2].Size, 500);

            //Parameter with different name from the comparison field, automatic size by value length
            AssertEquals(actual[3].ParameterName, "@PersonalField3");
            AssertEquals(actual[3].Value, param3);
            AssertEquals(actual[3].Direction, ParameterDirection.Input);
            AssertEquals(actual[3].DbType, DbType.String);
            AssertEquals(((SqlParameter)actual[3]).SqlDbType, SqlDbType.NVarChar);
            AssertEquals(actual[3].Size, 500);

            AssertEquals(actual[4].ParameterName, "@DateProperty");
            AssertEquals(actual[4].Value, param4);
            AssertEquals(actual[4].Direction, ParameterDirection.Input);
            AssertEquals(actual[4].DbType, DbType.DateTimeOffset);
            AssertEquals(((SqlParameter)actual[4]).SqlDbType, SqlDbType.DateTimeOffset);

            AssertEquals(actual[5].ParameterName, "@DecimalProperty");
            AssertEquals(actual[5].Value, param5);
            AssertEquals(actual[5].Direction, ParameterDirection.Input);
            AssertEquals(actual[5].DbType, DbType.Decimal);
            AssertEquals(((SqlParameter)actual[5]).SqlDbType, SqlDbType.Decimal);

            AssertEquals(actual[6].ParameterName, "@FloatProperty");
            AssertEquals(actual[6].Value, param6);
            AssertEquals(actual[6].Direction, ParameterDirection.Input);
            AssertEquals(actual[6].DbType, DbType.Double);
            AssertEquals(((SqlParameter)actual[6]).SqlDbType, SqlDbType.Float);

            AssertEquals(actual[7].ParameterName, "@BooleanProperty");
            AssertEquals(actual[7].Value, param7);
            AssertEquals(actual[7].Direction, ParameterDirection.Input);
            AssertEquals(actual[7].DbType, DbType.Boolean);
            AssertEquals(((SqlParameter)actual[7]).SqlDbType, SqlDbType.Bit);

            AssertEquals(actual[8].ParameterName, "@GuidProperty");
            AssertEquals(actual[8].Value, param8);
            AssertEquals(actual[8].Direction, ParameterDirection.Input);
            AssertEquals(actual[8].DbType, DbType.Guid);
            AssertEquals(((SqlParameter)actual[8]).SqlDbType, SqlDbType.UniqueIdentifier);
        }
    }

    internal class FakeDocument : Item
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string PersonalField { get; set; }
    }
}