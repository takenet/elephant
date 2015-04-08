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
    public class SqlGuidSetFacts : GuidSetFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidSetFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }
            
        public override ISet<Guid> Create()
        {
            var table = new Table("Guids", new [] {"Value"}, new Dictionary<string, SqlType>() { { "Value", new SqlType(DbType.Guid) }});
            _fixture.DropTable(table.Name);
            return new SqlGuidSet(table, _fixture.ConnectionString);
        }

        private class SqlGuidSet : SqlSet<Guid>
        {
            public SqlGuidSet(ITable table, string connectionString) : base(table, connectionString)
            {
            }

            protected override IMapper<Guid> Mapper => new ValueMapper<Guid>("Value");
        }
    }
}
