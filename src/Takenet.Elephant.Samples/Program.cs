using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Takenet.Elephant.Memory;

namespace Takenet.Elephant.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine(@"Choose an implementation: ");
            foreach (Implementation value in Enum.GetValues(typeof(Implementation)))
            {
                Console.WriteLine($"{(int)value}. {value}");
            }
            Implementation option;
            if (!Enum.TryParse(Console.ReadLine(), out option))
                option = Implementation.Memory;

            Console.WriteLine($"Using the '{option}' implementation");

            IDataMap dataMap = GetDataMapImplementation(option);
            await RunMapSamplesAsync(dataMap);


            await RunSetMapSamplesAsync(new SetMap<Guid, Data>());

            Console.ReadLine();
        }

        private static async Task RunMapSamplesAsync(IMap<Guid, Data> map)
        {            
            var id = Guid.NewGuid();
            var data = new Data() {Name = "A name", Value = 5};

            // Adds without overwrite any existing value in the key
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

        private static async Task RunSetMapSamplesAsync(ISetMap<Guid, Data> setMap)
        {
            var id = Guid.NewGuid();
            var data = new Data() { Name = "A name", Value = 5 };

            // A SetMap is a map of sets with special extensions methods

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
    }
}
