using System.Threading.Tasks;

namespace Takenet.Elephant.Specialized
{
    public class MapScopeFactory : IScopeFactory
    {
        private readonly ISetMap<string, string> _scopeKeysSetMap;

        public MapScopeFactory(ISetMap<string, string> scopeKeysSetMap)
        {
            _scopeKeysSetMap = scopeKeysSetMap;
        }

        public Task<IScope> CreateScopeAsync(string name)
        {
            throw new System.NotImplementedException();
        }
    }
}