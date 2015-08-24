using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Takenet.Elephant.MongoDb
{
    public class StorageBase<TKey> : IDisposable
    {

        private IMongoClient _client;
        private readonly IMongoDatabase _database;
        private readonly string _name;

        public StorageBase(string name, string connectionString, string db)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                _client = new MongoClient();
            }
            else
            {
                _client = new MongoClient(connectionString);
            }

            _database = _client.GetDatabase(db);
            _name = name;
            
        }

        static StorageBase()
        {
            MongoConfig.Configure();
        }

        ~StorageBase()
        {
            Dispose(false);
        }

        protected virtual IMongoDatabase GetDatabase()
        {
            return _database;
        }

        protected virtual IMongoCollection<TKey> GetCollection()
        {
            return _database.GetCollection<TKey>(_name);
        }


        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _client = null;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }

    internal static class MongoConfig
    {
        public static void Configure()
        {
            RegisterConventions();
            RegisterGlobalSerializationRules();
            ConfigureEntities();
            ConfigureValueObjects();
        }

        private static void RegisterConventions()
        {
            var pack = new ConventionPack { new CamelCaseElementNameConvention(), new IgnoreIfNullConvention(false) };
            ConventionRegistry.Register("all", pack, t => true);
        }

        private static void RegisterGlobalSerializationRules()
        {
            BsonSerializer.UseNullIdChecker = true;
        }

        private static void ConfigureEntities()
        {
            BsonClassMap.RegisterClassMap<MongoBaseEntity>(cm =>
            {
                cm.AutoMap();
                cm.MapMember(c => c.Id).SetSerializer(new StringSerializer(BsonType.ObjectId));
                cm.SetIdMember(cm.GetMemberMap(c => c.Id));
            });
        }

        private static void ConfigureValueObjects()
        {
        }
    }
}
