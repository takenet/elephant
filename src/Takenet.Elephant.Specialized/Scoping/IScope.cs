using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized.Scoping
{
    public interface IScope
    {
        string Name { get; }

        Task ClearAsync();
    }
}
