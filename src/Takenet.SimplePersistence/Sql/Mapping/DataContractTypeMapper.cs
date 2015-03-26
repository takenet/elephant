using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public sealed class DataContractTypeMapper<TEntity> : TypeMapper<TEntity> where TEntity : class, new()
    {
        public DataContractTypeMapper(ITable table)
            : base(table, typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetCustomAttribute<DataMemberAttribute>() != null).ToArray())
        {

        }
    }
}