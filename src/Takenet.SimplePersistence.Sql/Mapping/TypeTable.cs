using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public class TypeTable<T> : PropertyInfoTable
    {
        public TypeTable()
            : this(typeof(T).Name, new[] {  typeof(T).Name + "Id" })
        {

        }

        public TypeTable(string name, string[] keyColumns)
            : base(name, keyColumns, typeof (T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            
        }
    }
}