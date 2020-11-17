// Copyright 2020 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        // from: https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/

        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void AvroWithReferences(Config config)
        {
            var srClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            Schema schema1 = new Schema(@"{""type"":""record"",""name"":""EventA"",""namespace"":""Kafka.Avro.Examples"",""fields"":[{""name"":""EventType"",""type"":""string""},{""name"":""EventId"",""type"":""string""},{""name"":""OccuredOn"",""type"":""long""},{""name"":""A"",""type"":""string""}]}", SchemaType.Avro);
            Schema schema2 = new Schema(@"{""type"":""record"",""name"":""EventB"",""namespace"":""Kafka.Avro.Examples"",""fields"":[{""name"":""EventType"",""type"":""string""},{""name"":""EventId"",""type"":""string""},{""name"":""OccuredOn"",""type"":""long""},{""name"":""B"",""type"":""long""}]}", SchemaType.Avro);
            var id1 = srClient.RegisterSchemaAsync("events-a", schema1).Result;
            var id2 = srClient.RegisterSchemaAsync("events-b", schema2).Result;

            var avroUnion = @"[""Kafka.Avro.Examples.EventA"",""Kafka.Avro.Examples.EventB""]";
            Schema unionSchema = new Schema(avroUnion, SchemaType.Avro);
            SchemaReference reference = new SchemaReference(
              "Kafka.Avro.Examples.EventA",
              "events-a",
              1);
            unionSchema.References.Add(reference);
            reference = new SchemaReference(
              "Kafka.Avro.Examples.EventB",
              "events-b",
              1);
            unionSchema.References.Add(reference);

            var id3 = srClient.RegisterSchemaAsync("events-value", unionSchema).Result;
						Assert.NotEqual(0, id3);
        }
    }
}
