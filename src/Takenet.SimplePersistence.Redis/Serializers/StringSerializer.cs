using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.SimplePersistence.Redis.Serializers
{
    public sealed class StringSerializer : ISerializer<string>
    {
        #region ISerializer<string> Members

        public string Serialize(string value)
        {
            return value;
        }

        public string Deserialize(string value)
        {
            return value;
        }

        #endregion
    }
}
