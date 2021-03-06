﻿using System;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Serializers;
using Xunit;

namespace Take.Elephant.Tests.Redis
{
    [Trait("Category", nameof(Redis))]
    [Collection(nameof(Redis))]
    public class RedisGuidItemPositionListFacts : GuidItemPositionListFacts
    {
        private readonly RedisFixture _redisFixture;
        public const string ListName = "guid-list-items";

        public RedisGuidItemPositionListFacts(RedisFixture redisFixture)
        {
            _redisFixture = redisFixture;
        }

        public override IPositionList<Guid> Create()
        {
            int db = 2;
            _redisFixture.Server.FlushDatabase(db);
            return new RedisList<Guid>(ListName, _redisFixture.Connection.Configuration, new ValueSerializer<Guid>(), db);
        }
    }
}