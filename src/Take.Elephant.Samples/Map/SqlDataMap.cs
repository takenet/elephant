using System;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Samples.Map
{
    public class SqlDataMap : SqlMap<Guid, Data>, IDataMap
    {
        private static readonly ITable table;

        static SqlDataMap()
        {
            table = TableBuilder
                .WithName("Data")
                .WithKeyColumnFromType<Guid>("Id")
                .WithColumnsFromTypeProperties<Data>()
                .Build();
        }

        public SqlDataMap()
            : base(@"Server=(localdb)\MSSQLLocalDB;Database=Elephant;Integrated Security=true",
                table, new ValueMapper<Guid>("Id"), new TypeMapper<Data>(table))
        {
            UseFullyAsyncEnumerator = true;
        }
    }
}