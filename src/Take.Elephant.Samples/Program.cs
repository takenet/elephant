using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Take.Elephant.Memory;
using Take.Elephant.Samples.Map;
using Take.Elephant.Samples.Set;
using Take.Elephant.Samples.SetMap;

namespace Take.Elephant.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(@"Choose an implementation: ");
            Console.ForegroundColor = ConsoleColor.White;

            foreach (Implementation value in Enum.GetValues(typeof(Implementation)))
            {
                Console.WriteLine($"{(int)value}. {value}");
            }
            Implementation option;
            if (!Enum.TryParse(Console.ReadLine(), out option))
                option = Implementation.Memory;

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Using the '{option}' implementation");
            Console.ForegroundColor = ConsoleColor.White;

            // Run map samples
            IDataMap dataMap = GetDataMapImplementation(option);
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("Map samples");
            Console.ForegroundColor = ConsoleColor.White;
            await RunMapSamplesAsync(dataMap);

            // Run set samples
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("Set samples");
            Console.ForegroundColor = ConsoleColor.White;
            IDataSet dataSet = GetDataSetImplementation(option);
            await RunSetSamplesAsync(dataSet);

            // Run set map samples
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("SetMap samples");
            Console.ForegroundColor = ConsoleColor.White;
            IDataSetMap dataSetMap = GetDataSetMapImplementation(option);
            await RunSetMapSamplesAsync(dataSetMap);

            Console.ReadLine();
        }

        private static async Task RunMapSamplesAsync(IMap<Guid, Data> map)
        {            
            var id = Guid.NewGuid();
            var data = new Data() {Name = "A name", Value = 5};

            // Adds without overwriting any existing value in the key
            if (await map.TryAddAsync(id, data))
                Console.WriteLine($"A value for key '{id}' was set successfully");

            var newData = new Data() {Name = "A new name", Value = 1};

            // Adds the value, overwriting if the key is already set
            if (await map.TryAddAsync(id, newData, true))
                Console.WriteLine($"A new value for the key '{id}' was set successfully");

            // Gets the value (or the type default) for the key
            var existingData = await map.GetValueOrDefaultAsync(id);

            // Checks if the key exists
            if (await map.ContainsKeyAsync(id))
                Console.WriteLine($"The key '{id}' exists in the map");

            // Removes the value defined in the key
            if (await map.TryRemoveAsync(id))
                Console.WriteLine($"The value for the key '{id}' was removed");
        }

        private static async Task RunSetSamplesAsync(ISet<Data> set)
        {
            var data = new Data() { Name = "A name", Value = 5 };

            // A set is a collection of unique items
            await set.AddAsync(data);

            // Adding the same item again overwrites the existing. 
            // Usually this doesn't makes any difference, but depend of how the implementation compares the values.
            // For instance, the memory set uses an equality comparer, and the SQL uses the item's primary key values.
            await set.AddAsync(data);

            if (await set.ContainsAsync(data))
                Console.WriteLine($"The value '{data}' exists in the set");

            // The set also supports the IAsyncEnumerable interface, that allows async enumeration of the items.
            IAsyncEnumerable<Data> enumerable = await set.AsEnumerableAsync();

            // Some async extensions are available
            Console.WriteLine($"There are '{await enumerable.CountAsync()}' items in the set, which are:");
            await enumerable.ForEachAsync(i => Console.WriteLine($"- {i}"), CancellationToken.None);

        }

        private static async Task RunSetMapSamplesAsync(ISetMap<Guid, Data> setMap)
        {
            var id = Guid.NewGuid();
            var data = new Data() { Name = "A name", Value = 5 };

            // A SetMap is a map of sets items with special extensions methods
            // Uses an extension method that adds an item to a set in the key
            await setMap.AddItemAsync(id, data);
            var set = await setMap.GetValueOrDefaultAsync(id);
            if (await set.ContainsAsync(data))
                Console.WriteLine($"The value '{data}' is present in the set");

            // Removes the item from the set map
            if (await setMap.TryRemoveItemAsync(id, data))
                Console.WriteLine($"The item of the set in the key '{id}' was removed");

        }

        public static IDataMap GetDataMapImplementation(Implementation type)
        {
            switch (type)
            {                
                case Implementation.Redis:
                    return new RedisDataMap();
                case Implementation.Sql:
                    return new SqlDataMap();
                default:
                    return new MemoryDataMap();
            }
        }

        public static IDataSet GetDataSetImplementation(Implementation type)
        {
            switch (type)
            {
                case Implementation.Redis:
                    return new RedisDataSet();
                case Implementation.Sql:
                    return new SqlDataSet();
                default:
                    return new MemoryDataSet();
            }
        }

        public static IDataSetMap GetDataSetMapImplementation(Implementation type)
        {
            switch (type)
            {
                case Implementation.Redis:
                    return new RedisDataSetMap();
                case Implementation.Sql:
                    return new SqlDataSetMap();
                default:
                    return new MemoryDataSetMap();
            }
        }
    }
}
