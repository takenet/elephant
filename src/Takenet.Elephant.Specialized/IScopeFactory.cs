using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public interface IScopeFactory
    {
        Task<IScope> CreateScopeAsync(string name);
    }
}