﻿using System;
using System.Data;
using Take.Elephant.Sql;
using Take.Elephant.Sql.Mapping;

namespace Take.Elephant.Tests.Sql
{
    public abstract class SqlGuidItemMapFacts : GuidItemMapFacts
    {
        private readonly ISqlFixture _serverFixture;

        protected SqlGuidItemMapFacts(ISqlFixture serverFixture)
        {
            _serverFixture = serverFixture;
        }

        public override IMap<Guid, Item> Create()
        {
            var table = TableBuilder
                .WithName("GuidItems")
                .WithColumnsFromTypeProperties<Item>(p => !p.Name.Equals(nameof(Item.StringProperty)))
                .WithColumn(nameof(Item.StringProperty), new SqlType(DbType.String, int.MaxValue))
                .WithKeyColumnFromType<Guid>("Key")
                .Build();
            _serverFixture.DropTable(table.Schema, table.Name);

            var keyMapper = new ValueMapper<Guid>("Key");
            var valueMapper = new TypeMapper<Item>(table);
            return Create(table, keyMapper, valueMapper);
        }

        protected virtual IMap<Guid, Item> Create(ITable table, ValueMapper<Guid> keyMapper, TypeMapper<Item> valueMapper) =>
            new SqlMap<Guid, Item>(_serverFixture.DatabaseDriver, _serverFixture.ConnectionString, table, keyMapper, valueMapper);
    }
}
