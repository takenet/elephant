using System.Threading.Tasks;

namespace Take.Elephant.Specialized.Scoping
{
    public interface IScopedMap
    {
        string Identifier { get; }

        Task RemoveKeyAsync(string key);
    }
}