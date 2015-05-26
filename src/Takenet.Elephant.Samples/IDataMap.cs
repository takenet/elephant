using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;
using Takenet.Elephant.Redis;
using Takenet.Elephant.Redis.Converters;
using Takenet.Elephant.Sql;
using Takenet.Elephant.Sql.Mapping;

namespace Takenet.Elephant.Samples
{
    public interface IDataMap : IMap<Guid, Data>, IPropertyMap<Guid, Data>
    {

    }

    public class MemoryDataMap : Map<Guid, Data>, IDataMap
    {

    }

    public class RedisDataMap : RedisHashMap<Guid, Data>, IDataMap
    {
        public RedisDataMap() 
            : base("data", new TypeRedisDictionaryConverter<Data>(), "localhost")
        {

        }
    }

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
            : base(@"Server=(localdb)\v12.0;Database=Elephant;Integrated Security=true",
                table, new ValueMapper<Guid>("Id"), new TypeMapper<Data>(table))
        {
               
        }
    }
}
