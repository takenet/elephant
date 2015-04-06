using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlStringSetFacts : StringSetFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlStringSetFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }
            
        public override ISet<string> Create()
        {
            var table = new Table("Strings", new [] {"Value"}, new Dictionary<string, SqlType>() { { "Value", new SqlType(DbType.String) }});
            _fixture.DropTable(table.Name);
            return new SqlStringSet(table, _fixture.ConnectionString);
        }

        private class SqlStringSet : SqlSet<string>
        {
            public SqlStringSet(ITable table, string connectionString) : base(table, connectionString)
            {
            }

            protected override IMapper<string> Mapper => new ValueMapper<string>("Value");
        }
    }
}
