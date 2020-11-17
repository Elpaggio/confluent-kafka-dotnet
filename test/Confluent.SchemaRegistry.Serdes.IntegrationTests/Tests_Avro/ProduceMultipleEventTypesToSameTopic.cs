// Copyright 2018 Confluent Inc.
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
using Confluent.Kafka.Examples.AvroSpecific;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void ProduceMultipleEventTypesToSameTopic(string bootstrapServers, string schemaRegistryServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryServers
            };

            var adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };

           var topic = Guid.NewGuid().ToString();

            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
            }

            var srClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
            Schema schema1 = new Schema(@"{""type"":""record"",""name"":""EventA"",""namespace"":""Kafka.Avro.Examples"",""fields"":[{""name"":""EventType"",""type"":""string""},{""name"":""EventId"",""type"":""string""},{""name"":""OccuredOn"",""type"":""long""},{""name"":""A"",""type"":""string""}]}", SchemaType.Avro);
            Schema schema2 = new Schema(@"{""type"":""record"",""name"":""EventB"",""namespace"":""Kafka.Avro.Examples"",""fields"":[{""name"":""EventType"",""type"":""string""},{""name"":""EventId"",""type"":""string""},{""name"":""OccuredOn"",""type"":""long""},{""name"":""B"",""type"":""long""}]}", SchemaType.Avro);
            var id1 = srClient.RegisterSchemaAsync("events-a", schema1).Result;
            var id2 = srClient.RegisterSchemaAsync("events-b", schema2).Result;

            var avroUnion = @"[""Kafka.Avro.Examples.EventA"",""Kafka.Avro.Examples.EventB""]";
            Schema unionSchema = new Schema(avroUnion, SchemaType.Avro);
            SchemaReference reference = new SchemaReference(
              "Kafka.Avro.Examples.EventA",
              "events-a",
              srClient.GetLatestSchemaAsync("events-a").Result.Version);
            unionSchema.References.Add(reference);
            reference = new SchemaReference(
              "Kafka.Avro.Examples.EventB",
              "events-b",
              srClient.GetLatestSchemaAsync("events-b").Result.Version);
            unionSchema.References.Add(reference);

            var id3 = srClient.RegisterSchemaAsync($"{topic}-value", unionSchema).Result;

            AvroSerializerConfig avroSerializerConfig = new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestSchemaVersion = true };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
               new ProducerBuilder<Null, EventA>(producerConfig)
                   .SetValueSerializer(new AvroSerializer<EventA>(schemaRegistry, avroSerializerConfig))
                   .Build())
            {
                var eventA = new EventA
                {
                    A = "I'm event A",
                    EventId = Guid.NewGuid().ToString(),
                    EventType = "EventType-A",
                    OccuredOn = DateTime.UtcNow.Ticks,
                };

                producer.ProduceAsync(topic, new Message<Null, EventA> { Value = eventA }).Wait();

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
               new ProducerBuilder<Null, EventB>(producerConfig)
                   .SetValueSerializer(new AvroSerializer<EventB>(schemaRegistry, avroSerializerConfig))
                   .Build())
            {
                var eventB = new EventB
                {
                    B = 123456987,
                    EventId = Guid.NewGuid().ToString(),
                    EventType = "EventType-A",
                    OccuredOn = DateTime.UtcNow.Ticks,
                };

                producer.ProduceAsync(topic, new Message<Null, EventB> { Value = eventB }).Wait();

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }
        }
    }
}
