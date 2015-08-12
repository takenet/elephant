using System;
using Takenet.Elephant.Redis.Serializers;

namespace Takenet.Elephant.Tests.Specialized
{
    public abstract class GuidItemScopedMapFacts : ScopedMapFacts<Guid, Item>
    {
    }
}