using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Take.Elephant.Memory;
using Take.Elephant.Redis;
using Take.Elephant.Specialized.Cache;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;
using Take.Elephant.Tests.Redis;
using Xunit;

namespace Take.Elephant.Tests.Specialized
{
    public class SetMapAccessCountDecorator : ISetMap<Guid, Item>
    {
        private readonly ISetMap<Guid, Item> _setMap;

        public SetMapAccessCountDecorator(ISetMap<Guid, Item> setMap)
        {
            _setMap = setMap;
        }

        public int ReadCount { get; private set; }

        public bool SupportsEmptySets => _setMap.SupportsEmptySets;

        public Task<bool> ContainsKeyAsync(Guid key, CancellationToken cancellationToken = default)
        {
            return _setMap.ContainsKeyAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<ISet<Item>> GetValueOrDefaultAsync(Guid key, CancellationToken cancellationToken = default)
        {
            return _setMap.GetValueOrDefaultAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<ISet<Item>> GetValueOrEmptyAsync(Guid key, CancellationToken cancellationToken = default)
        {
            return _setMap.GetValueOrEmptyAsync(key, cancellationToken).ContinueWith(t => { if (!t.IsFaulted) ReadCount++; return t.Result; });
        }

        public Task<bool> TryAddAsync(Guid key, ISet<Item> value, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            return _setMap.TryAddAsync(key, value, overwrite, cancellationToken);
        }

        public Task<bool> TryRemoveAsync(Guid key, CancellationToken cancellationToken = default)
        {
            return _setMap.TryRemoveAsync(key, cancellationToken);
        }
    }

    [Collection("SqlRedis")]
    public class SqlRedisGuidItemOnDemandCacheSetMapFacts : GuidItemOnDemandCacheSetMapFacts, IDisposable
    {
        private readonly SqlRedisFixture _fixture;
        public const string MapName = "guid-items";

        public SqlRedisGuidItemOnDemandCacheSetMapFacts(SqlRedisFixture fixture)
        {
            _fixture = fixture;
        }

        public override IMap<Guid, ISet<Item>> CreateSource()
        {
            var databaseDriver = new SqlDatabaseDriver();
            var columns = typeof(Item)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToSqlColumns();
            columns.Add("Key", new SqlType(DbType.Guid));
            var table = new Table("GuidItems", new[] { "Key", nameof(Item.GuidProperty) }, columns);
            _fixture.SqlConnectionFixture.DropTable(table.Schema, table.Name);
            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return new SetMapAccessCountDecorator(new SqlSetMap<Guid, Item>(databaseDriver, _fixture.SqlConnectionFixture.ConnectionString, table, keyMapper, valueMapper));
        }

        public override IMap<Guid, ISet<Item>> CreateCache()
        {
            var db = 1;
            _fixture.RedisFixture.Server.FlushDatabase(db);
            var setMap = new RedisSetMap<Guid, Item>(MapName, _fixture.RedisFixture.Connection.Configuration, new ItemSerializer(), db);
            return setMap;
        }

        public override ISet<Item> CreateValue(Guid key, bool populate)
        {
            var set = new Set<Item>();
            if (populate)
            {
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
                set.AddAsync(Fixture.Create<Item>()).Wait();
            }
            return set;
        }


        [Fact]
        public async Task TestPerformance()
        {
            (string scenarioName,
                bool emptySupport,
                bool usePredictor,
                int nonEmptySetsCount,
                int emptySetsCount,
                double emptySetIndicatorProbability,
                int sourceOnlySetsCount)[] scenarios =
            new[]
            {
                //("Scenario 1 - Original", false, false, 500, 500, 0.5, 200),
                //("Scenario 2 - Original", false, false, 500, 1000, 0.7, 200),
                //("Scenario 3 - Original", false, false, 500, 1000, 0.2, 200),
                //("Scenario 4 - Original", false, false, 200, 3000, 0.0, 200),
                ("Scenario 1 - Empty Support", true, false, 500, 500, 0.5, 200),
                ("Scenario 2 - Empty Support", true, false, 500, 1000, 0.7, 200),
                ("Scenario 3 - Empty Support", true, false, 500, 1000, 0.2, 200),
                ("Scenario 4 - Empty Support", true, false, 200, 3000, 0.9, 200),
                //("Scenario 1 - Empty + Predictor", true, true, 500, 500, 0.5, 200),
                //("Scenario 2 - Empty + Predictor", true, true, 500, 1000, 0.7, 200),
                //("Scenario 3 - Empty + Predictor", true, true, 500, 1000, 0.2, 200),
                //("Scenario 4 - Empty + Predictor", true, true, 200, 3000, 0.9, 200),
                //("Scenario 5 - Original", false, false, 0, 0, 0.0, 1000),
                //("Scenario 6 - Original", false, false, 1000, 0, 0.0, 0),
                //("Scenario 6 - Empty", true, false, 1000, 0, 0.0, 0),
            };

            var runs = new ConcurrentDictionary<string, System.Collections.Generic.List<(long takenMs, int readCount, int keysCount)>>();

            var directoryPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), @$"redis-tests\redis-test-{DateTime.Now:yyyy'-'MM'-'dd'T'HH'-'mm'-'ss}");
            Directory.CreateDirectory(directoryPath);

            // warm-up
            for (int i = 0; i < 5; i++)
            {
                await RunTest(scenarios, runs, directoryPath, report: false);
            }

            // actual
            const int sampleSize = 100;
            for (int i = 0; i < sampleSize; i++)
            {
                await RunTest(scenarios, runs, directoryPath);
            }

            var resultPath = Path.Combine(directoryPath, $"results-{DateTime.Now:yyyy'-'MM'-'dd'T'HH'-'mm'-'ss}.json");
            var results = runs.OrderBy(kvp => kvp.Key).Select(kvp =>
            {
                double takenMsAvg = kvp.Value.Average(v => v.takenMs);
                double takenMsMedian = kvp.Value.Median(v => v.takenMs);
                double takenMsStdDev = kvp.Value.StdDev(v => v.takenMs);
                double takenMsStdErr = takenMsStdDev / Math.Sqrt(kvp.Value.Count);
                double readCountAvg = kvp.Value.Average(v => v.readCount);
                double readCountStdDev = kvp.Value.StdDev(v => v.readCount);
                double readCountStdErr = readCountStdDev / Math.Sqrt(kvp.Value.Count);
                double readCountMedian = kvp.Value.Median(v => v.readCount);
                double keysCountAvg = kvp.Value.Average(v => v.keysCount);
                double keysCountStdDev = kvp.Value.StdDev(v => v.keysCount);
                double keysCountStdErr = keysCountStdDev / Math.Sqrt(kvp.Value.Count);
                double keysCountMedian = kvp.Value.Median(v => v.keysCount);

                return new
                {
                    scenario = kvp.Key,
                    takenMsAvg,
                    takenMsMedian,
                    takenMsStdDev,
                    takenMsStdErr,
                    readCountAvg,
                    readCountMedian,
                    readCountStdDev,
                    readCountStdErr,
                    keysCountAvg,
                    //keysCountMedian,
                    //keysCountStdDev,
                    //keysCountStdErr,

                };
            });

            await File.AppendAllTextAsync(resultPath, JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
        }

        private async Task RunTest((string scenarioName, bool emptySupport, bool usePredictor, int nonEmptySetsCount, int emptySetsCount, double emptySetIndicatorProbability, int sourceOnlySetsCount)[] scenarios,
                                   ConcurrentDictionary<string, System.Collections.Generic.List<(long takenMs, int readCount, int keysCount)>> runs,
                                   string directoryPath,
                                   bool report = true)
        {
            foreach (var scenario in scenarios)
            {
                // Arrange
                var db = _fixture.RedisFixture.Connection.GetDatabase(1);
                var source = CreateSource() as SetMapAccessCountDecorator;
                var cache = CreateCache() as RedisSetMap<Guid, Item>;
                var serializer = new ItemSerializer();
                //SetEmptySupport(cache, scenario.emptySupport, scenario.usePredictor);
                await ClearDbs();
                Setup(db, CreateSource() as SetMapAccessCountDecorator, serializer, scenario.nonEmptySetsCount, scenario.emptySetsCount, scenario.emptySetIndicatorProbability, scenario.sourceOnlySetsCount, out var keys, out var tasks);

                var setMap = Create(source, cache, TimeSpan.FromMinutes(15)) as OnDemandCacheSetMap<Guid, Item>;

                await Task.WhenAll(tasks);
                tasks.Clear();

                // Act
                var sw = new Stopwatch();
                sw.Start();
                foreach (var batch in BatchBy(keys, 100))
                {
                    await Task.WhenAll(batch.Select(key => setMap.GetValueOrDefaultAsync(key)));
                }

                sw.Stop();

                await ClearDbs();

                // A...Report
                if (report)
                {
                    runs.GetOrAdd(scenario.scenarioName, new System.Collections.Generic.List<(long, int, int)>()).Add((sw.ElapsedMilliseconds, source.ReadCount, keys.Count));

                    var fileName = scenario.scenarioName.Split('-').Last().Trim() + ".txt";
                    var filePath = Path.Combine(directoryPath, fileName);
                    await File.AppendAllTextAsync(filePath,
    $@"
{DateTime.Now}
{scenario.scenarioName.Split('-').Last().Trim()}
Time taken: {sw.ElapsedMilliseconds}ms
Source read count: {source.ReadCount}

-----------------------

");
                }
            }
        }

        private async Task ClearDbs()
        {
            await _fixture.RedisFixture.Server.FlushAllDatabasesAsync();
            var connection = _fixture.SqlConnectionFixture.Connection;
            var command = connection.CreateCommand();
            command.CommandText = "IF OBJECT_ID('dbo.GuidItems', 'U') IS NOT NULL  delete from GuidItems";
            await command.ExecuteNonQueryAsync();
        }

        //private void SetEmptySupport(RedisSetMap<Guid, Item> cache, bool emptySupport, bool usePredictor)
        //{
        //    cache._supportEmptySets = emptySupport;
        //    cache._predictor = usePredictor ? new RedisSetMap<Guid, Item>.EmptySetPredictor() : null;
        //    RedisSet<Item>._supportEmptySets = emptySupport;
        //}

        private void Setup(StackExchange.Redis.IDatabase redis,
                           ISetMap<Guid, Item> sql,
                           ItemSerializer serializer,
                           int nonEmptySetsCount,
                           int emptySetsCount,
                           double emptySetIndicatorProbability,
                           int sourceOnlySetsCount,
                           out System.Collections.Generic.List<Guid> keys,
                           out System.Collections.Generic.List<Task> tasks)
        {
            keys = new System.Collections.Generic.List<Guid>();
            var random = new Random();

            tasks = new System.Collections.Generic.List<Task>();
            for (var i = 0; i < nonEmptySetsCount; i++)
            {
                var key = Guid.NewGuid();
                keys.Add(key);
                for (var j = 0; j < 4; j++)
                {
                    var value = Fixture.Create<Item>();
                    tasks.Add(redis.SetAddAsync($"guid-items:{key}", serializer.Serialize(value)));
                    tasks.Add(sql.AddItemAsync(key, value));

                    if (random.NextDouble() < emptySetIndicatorProbability)
                    {
                        tasks.Add(redis.StringSetAsync($"guid-items:{key}__ELEPHANT_EMPTY_SET_INDICATOR__", false.ToString()));
                    }
                }
            }

            for (var i = 0; i < emptySetsCount; i++)
            {
                var key = Guid.NewGuid();
                keys.Add(key);
                if (random.NextDouble() < emptySetIndicatorProbability)
                {
                    tasks.Add(redis.StringSetAsync($"guid-items:{key}__ELEPHANT_EMPTY_SET_INDICATOR__", true.ToString()));
                }
            }

            for (var i = 0; i < sourceOnlySetsCount; i++)
            {
                var key = Guid.NewGuid();
                keys.Add(key);
                tasks.Add(sql.AddItemAsync(key, Fixture.Create<Item>()));
            }
        }

        public IEnumerable<IEnumerable<T>> BatchBy<T>(IEnumerable<T> items, int batchSize)
        {
            var count = 0;
            return items.GroupBy(x => (count++ / batchSize)).ToList();
        }

        public void Dispose()
        {
            _fixture.RedisFixture.Server.FlushAllDatabases();
        }
    }

    public static class Extensions
    {
        public static double StdDev<T>(this IEnumerable<T> values, Func<T, double> selector)
        {
            // ref: https://stackoverflow.com/questions/2253874/linq-equivalent-for-standard-deviation
            // ref: http://warrenseen.com/blog/2006/03/13/how-to-calculate-standard-deviation/ 
            var mean = 0.0;
            var sum = 0.0;
            var stdDev = 0.0;
            var n = 0;
            foreach (var value in values.Select(selector))
            {
                n++;
                var delta = value - mean;
                mean += delta / n;
                sum += delta * (value - mean);
            }
            if (1 < n)
                stdDev = Math.Sqrt(sum / (n - 1));

            return stdDev;
        }

        public static double Median<T>(this IEnumerable<T> values, Func<T, double> selector)
        {
            int count = values.Count();

            if (count % 2 == 0)
                return values.Select(selector).OrderBy(x => x).Skip((count / 2) - 1).Take(2).Average();
            else
                return values.Select(selector).OrderBy(x => x).ElementAt(count / 2);
        }
    }
}