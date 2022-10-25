using System;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Samples.SetMap
{
    public class SqlDataSetMap : SqlSetMap<Guid, Data>, IDataSetMap
    {
        private static readonly ITable table;

        static SqlDataSetMap()
        {
            table = TableBuilder
                .WithName("DataSetMap")
                .WithKeyColumnFromType<Guid>("Id")
                .WithKeyColumnsFromTypeProperties<Data>()
                .Build();
        }

        public SqlDataSetMap() 
            : base(@"Server=(localdb)\MSSQLLocalDB;Database=Elephant;Integrated Security=true",
                table, new ValueMapper<Guid>("Id"), new TypeMapper<Data>(table))
        {
            UseFullyAsyncEnumerator = true;
        }
    }
}