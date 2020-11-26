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
using System.IO;
using System.Linq;
using System.Net;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.SyncOverAsync;
using Newtonsoft.Json;
using Xunit;


namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        /// Test that we can produce multiple events to the same topic also supporting validation
        /// https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        private static void BMSpecificProduceConsume(string bootstrapServers, string schemaRegistryServers)
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
            Console.Error.WriteLine($"topic: {topic}");

            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
            }

            var srClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
            Schema schema1 = new Schema(EventA._SCHEMA.ToString(), SchemaType.Avro);
            Schema schema2 = new Schema(EventB._SCHEMA.ToString(), SchemaType.Avro);
            var id1 = srClient.RegisterSchemaAsync("events-a", schema1).Result;
            var id2 = srClient.RegisterSchemaAsync("events-b", schema2).Result;

            var avroUnion = @"[""Confluent.Kafka.Examples.AvroSpecific.EventA"",""Confluent.Kafka.Examples.AvroSpecific.EventB""]";
            Schema unionSchema = new Schema(avroUnion, SchemaType.Avro);
            SchemaReference reference = new SchemaReference(
              "Confluent.Kafka.Examples.AvroSpecific.EventA",
              "events-a",
              srClient.GetLatestSchemaAsync("events-a").Result.Version);
            unionSchema.References.Add(reference);
            reference = new SchemaReference(
              "Confluent.Kafka.Examples.AvroSpecific.EventB",
              "events-b",
              srClient.GetLatestSchemaAsync("events-b").Result.Version);
            unionSchema.References.Add(reference);

            var id3 = srClient.RegisterSchemaAsync($"{topic}-value", unionSchema).Result;

            AvroSerializerConfig avroSerializerConfig = new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestSchemaVersion = true };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
               new ProducerBuilder<string, ISpecificRecord>(producerConfig)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new BmSpecificSerializerImpl<ISpecificRecord>(
                       schemaRegistry,
                       false,
                       1024,
                       SubjectNameStrategy.Topic.ToDelegate(),
                       true))
                .Build())
            {
                for (int i = 0; i < 3; ++i)
                {
                    var eventA = new EventA
                    {
                        A = "I'm event A",
                        EventId = Guid.NewGuid().ToString(),
                        EventType = "EventType-A",
                        OccuredOn = DateTime.UtcNow.Ticks,
                    };

                    producer.ProduceAsync(topic, new Message<string, ISpecificRecord> { Key = "DomainEvent", Value = eventA }).Wait();
                }

                for (int i = 0; i < 3; ++i)
                {
                    var eventB = new EventB
                    {
                        B = 123456987,
                        EventId = Guid.NewGuid().ToString(),
                        EventType = "EventType-B",
                        OccuredOn = DateTime.UtcNow.Ticks,
                    };

                    producer.ProduceAsync(topic, new Message<string, ISpecificRecord> { Key = "DomainEvent", Value = eventB }).Wait();
                }

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new BmGenericDeserializerImpl(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Assert.True(false, e.Reason))
                    .Build())
            {
                consumer.Subscribe(topic);

                int i = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }

                    Console.WriteLine(record.Message.Value["EventType"]);
                    i += 1;
                }

                Assert.Equal(6, i);

                consumer.Close();
            }
        }

        [Theory, MemberData(nameof(TestParameters))]
        private static void GenericProduceConsume(string bootstrapServers, string schemaRegistryServers)
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
            Console.Error.WriteLine($"topic: {topic}");

            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                adminClient.CreateTopicsAsync(
                    new List<TopicSpecification> { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
            }

            var srClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
            Schema schema1 = new Schema(EventA._SCHEMA.ToString(), SchemaType.Avro);
            Schema schema2 = new Schema(EventB._SCHEMA.ToString(), SchemaType.Avro);
            var id1 = srClient.RegisterSchemaAsync("events-a", schema1).Result;
            var id2 = srClient.RegisterSchemaAsync("events-b", schema2).Result;

            var avroUnion = @"[""Confluent.Kafka.Examples.AvroSpecific.EventA"",""Confluent.Kafka.Examples.AvroSpecific.EventB""]";
            Schema unionSchema = new Schema(avroUnion, SchemaType.Avro);
            SchemaReference reference = new SchemaReference(
              "Confluent.Kafka.Examples.AvroSpecific.EventA",
              "events-a",
              srClient.GetLatestSchemaAsync("events-a").Result.Version);
            unionSchema.References.Add(reference);
            reference = new SchemaReference(
              "Confluent.Kafka.Examples.AvroSpecific.EventB",
              "events-b",
              srClient.GetLatestSchemaAsync("events-b").Result.Version);
            unionSchema.References.Add(reference);

            var id3 = srClient.RegisterSchemaAsync($"{topic}-value", unionSchema).Result;

            AvroSerializerConfig avroSerializerConfig = new AvroSerializerConfig { AutoRegisterSchemas = false, UseLatestSchemaVersion = true };
            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
               new ProducerBuilder<string, GenericRecord>(producerConfig)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new BmGenericSerializerImpl(
                       schemaRegistry,
                       false,
                       1024,
                       SubjectNameStrategy.Topic.ToDelegate(),
                       true))
                .Build())
            {
                for (int i = 0; i < 3; ++i)
                {
                    var eventA = new GenericRecord((Avro.RecordSchema)EventA._SCHEMA);
                    eventA.Add("A", "I'm event A");
                    eventA.Add("EventId", Guid.NewGuid().ToString());
                    eventA.Add("EventType", "EventType-A");
                    eventA.Add("OccuredOn", DateTime.UtcNow.Ticks);

                    producer.ProduceAsync(topic, new Message<string, GenericRecord> { Key = "DomainEvent", Value = eventA }).Wait();
                }

                for (int i = 0; i < 3; ++i)
                {
                    var eventB = new GenericRecord((Avro.RecordSchema)EventB._SCHEMA);
                    eventB.Add("B", 123456987L);
                    eventB.Add("EventId", Guid.NewGuid().ToString());
                    eventB.Add("EventType", "EventType-B");
                    eventB.Add("OccuredOn", DateTime.UtcNow.Ticks);

                    producer.ProduceAsync(topic, new Message<string, GenericRecord> { Key = "DomainEvent", Value = eventB }).Wait();
                }

                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new BmGenericDeserializerImpl(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Assert.True(false, e.Reason))
                    .Build())
            {
                consumer.Subscribe(topic);

                int i = 0;
                while (true)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record == null) { continue; }
                    if (record.IsPartitionEOF) { break; }

                    Console.WriteLine(record.Message.Value["EventType"]);
                    i += 1;
                }

                Assert.Equal(6, i);

                consumer.Close();
            }
        }

        // This should work (it does using the java client) according to https://avro.apache.org/docs/current/spec.html#Schema+Resolution
        // if writer's is a union, but reader's is not
        // If the reader's schema matches the selected writer's schema, it is recursively resolved against it. If they do not match, an error is signalled.

        // Unfortunately the .NET Avro doesn't support this scenario :(
        [Theory, MemberData(nameof(TestParameters))]
        private static void DefaultReader(string bootstrapServers, string schemaRegistryServers)
        {
            var writerSchema = Avro.Schema.Parse("[{\"type\":\"record\",\"name\":\"EventA\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific\",\"fields\":[{\"name\":\"EventType\",\"type\":\"string\"},{\"name\":\"EventId\",\"type\":\"string\"},{\"name\":\"OccuredOn\",\"type\":\"long\"},{\"name\":\"A\",\"type\":\"string\"}]}, {\"type\":\"record\",\"name\":\"EventB\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific\",\"fields\":[{\"name\":\"EventType\",\"type\":\"string\"},{\"name\":\"EventId\",\"type\":\"string\"},{\"name\":\"OccuredOn\",\"type\":\"long\"},{\"name\":\"B\",\"type\":\"long\"}]}]");
            var readerschema = EventA._SCHEMA;

            var eventA = new GenericRecord((Avro.RecordSchema)EventA._SCHEMA);
            eventA.Add("A", "I'm event A");
            eventA.Add("EventId", Guid.NewGuid().ToString());
            eventA.Add("EventType", "EventType-A");
            eventA.Add("OccuredOn", DateTime.UtcNow.Ticks);


            byte[] serializedBytes;
            using (var stream = new MemoryStream(1024))
            using (var writer = new BinaryWriter(stream))
            {
                stream.WriteByte(0);

                writer.Write(IPAddress.HostToNetworkOrder(1));
                new GenericWriter<GenericRecord>(writerSchema).Write(eventA, new BinaryEncoder(stream));

                // TODO: maybe change the ISerializer interface so that this copy isn't necessary.
                serializedBytes = stream.ToArray();
            }


            using (var stream = new MemoryStream(serializedBytes))
            using (var reader = new BinaryReader(stream))
            {
                var magicByte = reader.ReadByte();
                if (magicByte != 0)
                {
                    throw new InvalidDataException($"Expecting data with Confluent Schema Registry framing. Magic byte was {serializedBytes[0]}, expecting {0}");
                }
                var writerId = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                var datumReader = new GenericReader<GenericRecord>(writerSchema, readerschema);
                var rec = datumReader.Read(default(GenericRecord), new BinaryDecoder(stream));
            }
        }
    }
}
