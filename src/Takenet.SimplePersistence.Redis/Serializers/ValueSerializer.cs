using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Redis.Serializers
{
    public class ValueSerializer<T> : ISerializer<T>
    {
        private static Func<string, T> _parseFunc;

        static ValueSerializer()
        {
            _parseFunc = TypeUtil.GetParseFunc<T>();                        
        } 

        public string Serialize(T value)
        {
            return value.ToString();
        }

        public T Deserialize(string value)
        {
            return _parseFunc(value);
        }
    }
}
