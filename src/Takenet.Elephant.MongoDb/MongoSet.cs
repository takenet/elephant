using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;

namespace Takenet.Elephant.MongoDb
{
    public class MongoSet<T> : StorageBase<T>, ISet<T> where T : MongoBaseEntity
    {

        public MongoSet(string name, string connectionString, string db) : base(name, connectionString, db)
        {

        }

        public Task AddAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var collection = GetCollection();

            return collection.InsertOneAsync(value);
        }

        public async Task<IAsyncEnumerable<T>> AsEnumerableAsync()
        {
            var collection = GetCollection();

            IEnumerable<T> values;
            values = await collection.Find(new BsonDocument()).ToListAsync<T>();

            return new AsyncEnumerableWrapper<T>(values);
        }

        public async Task<bool> ContainsAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var filter = Builders<T>.Filter.Eq("id", value.Id);
            var collection = GetCollection();

            var document = await collection.Find(filter).FirstAsync();
            return document != null ? true : false;
        }

        public Task<long> GetLengthAsync()
        {
            var collection = GetCollection();

            return collection.CountAsync(new BsonDocument());
        }

        public async Task<bool> TryRemoveAsync(T value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            var filter = Builders<T>.Filter.Eq("id", value.Id);
            var collection = GetCollection();

            var deleteResult = await collection.DeleteOneAsync(filter);
            return deleteResult.DeletedCount > 0 ? true : false;
        }
    }
}
