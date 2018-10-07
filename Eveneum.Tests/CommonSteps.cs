﻿using Eveneum.Tests.Infrastrature;
using System;
using System.Threading.Tasks;
using TechTalk.SpecFlow;

namespace Eveneum.Tests
{
    [Binding]
    class CommonSteps
    {
        private readonly CosmosDbContext Context;

        public CommonSteps(CosmosDbContext context)
        {
            this.Context = context;
        }

        [Given(@"an event store backed by non-partitioned collection")]
        public async Task GivenAnEventStoreBackedByNonPartitionedCollection()
        {
            this.Context.Partitioned = false;

            await this.Context.Initialize();
        }

        [Given(@"an event store backed by partitioned collection")]
        public async Task GivenAnEventStoreBackedByPartitionedCollection()
        {
            this.Context.Partitioned = true;
            this.Context.Partition = Guid.NewGuid().ToString();

            await this.Context.Initialize();
        }
    }
}
