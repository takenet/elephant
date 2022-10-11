using NFluent;
using System.Threading.Tasks;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Xunit;

namespace Take.Elephant.Tests.Sql.SqlServer
{
    public class SqlServerListFactIdentity
    {
        private readonly ISqlFixture _serverFixture;
        private const string TABLE_NAME = "ItemsSetIdentity";

        public SqlServerListFactIdentity()
        {
            _serverFixture = new SqlServerFixture();
        }

        public class Item
        {
            public long IdentityProperty { get; set; }

            public string StringProperty { get; set; }

            public int IntProperty { get; set; }
        }

        private IList<Item> Create()
        {
            var table = TableBuilder.WithName(TABLE_NAME)
                .WithKeyColumnFromType<long>(nameof(Item.IdentityProperty), true)
                .WithColumnsFromTypeProperties<Item>(p => !p.Name.Equals(nameof(Item.IdentityProperty)))
                .WithSynchronizationStrategy(SchemaSynchronizationStrategy.UntilSuccess)
                .Build();
            
            _serverFixture.DropTable(table.Schema, table.Name);

            var mapper = new TypeMapper<Item>(table);
            return new SqlList<Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, mapper);
        }
        
        [Fact(DisplayName = nameof(RemoveAllWithIdentitySucceeds))]
        public async Task RemoveAllWithIdentitySucceeds()
        {
            var item1 = new Item
            {
                IntProperty = 1,
                StringProperty = "First",
                IdentityProperty = 1
            };

            var item2 = new Item
            {
                IntProperty = 2,
                StringProperty = "Second",
                IdentityProperty = 2
            };

            var item3 = new Item
            {
                IntProperty = 3,
                StringProperty = "Third",
                IdentityProperty = 3
            };

            var list = Create();

            await list.AddAsync(item1);
            await list.AddAsync(item2);
            await list.AddAsync(item3);

            var result = await list.RemoveAllAsync(item1);
            Check.That(result).IsEqualTo(1);

            result = await list.RemoveAllAsync(item2);
            Check.That(result).IsEqualTo(1);

            var length = await list.GetLengthAsync();
            Check.That(length).IsEqualTo(1);

            _serverFixture.DropTable(null, TABLE_NAME);
        }
    }
}
