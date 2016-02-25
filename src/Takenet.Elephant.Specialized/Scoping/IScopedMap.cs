using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public interface IScopedMap
    {
        string Identifier { get; }

        Task RemoveKeyAsync(string key);
    }
}