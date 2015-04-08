using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Ploeh.AutoFixture;
using Takenet.SimplePersistence.Sql;
using Takenet.SimplePersistence.Sql.Mapping;
using Xunit;

namespace Takenet.SimplePersistence.Tests.Sql
{
    [Collection("Sql")]
    public class SqlGuidItemSetMapFacts : GuidItemSetMapFacts
    {
        private readonly SqlConnectionFixture _fixture;

        public SqlGuidItemSetMapFacts(SqlConnectionFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, ISet<Item>> Create()
        {
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns);
            _fixture.DropTable(table.Name);
            return new SqlGuidItemSetMap(table, _fixture.ConnectionString);
        }

        public override ISet<Item> CreateValue(Guid key)
        {
            var set = new SimplePersistence.Memory.Set<Item>();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            set.AddAsync(Fixture.Create<Item>()).Wait();
            return set;
        }

        private class SqlGuidItemSetMap : SqlSetMap<Guid, Item>
        {
            public SqlGuidItemSetMap(ITable table, string connectionString) 
                : base(table, connectionString)
            {
                                
            }
            protected override IMapper<Item> Mapper => new TypeMapper<Item>(Table);

            protected override IMapper<Guid> KeyMapper => new ValueMapper<Guid>("Key");
        }
    }
}
