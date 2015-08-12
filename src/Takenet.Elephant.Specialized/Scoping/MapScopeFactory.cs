namespace Takenet.Elephant.Specialized.Scoping
{
    public class MapScopeFactory : IScopeFactory
    {
        private readonly ISetMap<string, string> _scopeKeysSetMap;

        public MapScopeFactory(ISetMap<string, string> scopeKeysSetMap)
        {
            _scopeKeysSetMap = scopeKeysSetMap;
        }

        public IScope CreateScope(string name)
        {
            return new MapScope(name, _scopeKeysSetMap);
        }
    }
}