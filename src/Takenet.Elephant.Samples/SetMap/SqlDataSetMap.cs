using System;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Samples.SetMap
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
            : base(@"Server=(localdb)\v12.0;Database=Elephant;Integrated Security=true",
                table, new ValueMapper<Guid>("Id"), new TypeMapper<Data>(table))
        {
        }
    }
}