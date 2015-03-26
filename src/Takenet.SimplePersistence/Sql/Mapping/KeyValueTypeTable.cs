using System.Reflection;

namespace Takenet.SimplePersistence.Sql.Mapping
{
    public sealed class KeyValueTypeTable<TKey, TValue> : KeyValuePropertyInfoTable
    {
        public KeyValueTypeTable()
            : this(typeof(TKey).Name + typeof(TValue).Name)
        {

        }

        public KeyValueTypeTable(string name)
            : base(name, typeof(TKey).GetProperties(BindingFlags.Public | BindingFlags.Instance), typeof(TValue).GetProperties(BindingFlags.Public))
        {
        }
    }
}