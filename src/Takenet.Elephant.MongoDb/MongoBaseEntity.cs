using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Takenet.Elephant.MongoDb
{
    public abstract class MongoBaseEntity
    {
        protected MongoBaseEntity()
        {
            Id = ObjectId.GenerateNewId().ToString();
        }

        public string Id { get; private set; }
    }
}
