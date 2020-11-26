﻿// Copyright 2018 Confluent Inc.
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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Avro.Generic;
using Avro.IO;
using System.Linq;
using System.Text;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <inheritdoc/>
    public class BmGenericSerializerImpl : IAvroSerializerImpl<GenericRecord>, IAsyncSerializer<GenericRecord>
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private int initialBufferSize;
        private SubjectNameStrategyDelegate subjectNameStrategy;

        private bool useLatestSchema { get; }

        private Dictionary<global::Avro.Schema, string> knownSchemas = new Dictionary<global::Avro.Schema, string>();
        private Dictionary<global::Avro.Schema, global::Avro.Schema> unionSchemas = new Dictionary<global::Avro.Schema, global::Avro.Schema>();
        private HashSet<KeyValuePair<string, string>> registeredSchemas = new HashSet<KeyValuePair<string, string>>();
        private Dictionary<string, int> schemaIds = new Dictionary<string, int>();

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        /// <inheritdoc/>
        public BmGenericSerializerImpl(
            ISchemaRegistryClient schemaRegistryClient,
            bool autoRegisterSchema,
            int initialBufferSize,
            SubjectNameStrategyDelegate subjectNameStrategy,
            bool useLatestSchema = false)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            this.autoRegisterSchema = autoRegisterSchema;
            this.initialBufferSize = initialBufferSize;
            this.subjectNameStrategy = subjectNameStrategy;
            this.useLatestSchema = useLatestSchema;
        }

        /// <summary>
        ///     Serialize GenericRecord instance to a byte array in Avro format. The serialized
        ///     data is preceded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order). This call may block or throw 
        ///     on first use for a particular topic during schema registration.
        /// </summary>
        /// <param name="topic">
        ///     The topic associated with the data.
        /// </param>
        /// <param name="data">
        ///     The object to serialize.
        /// </param>
        /// <param name="isKey">
        ///     whether or not the data represents a message key.
        /// </param>
        /// <returns>
        ///     <paramref name="data" /> serialized as a byte array.
        /// </returns>
        public async Task<byte[]> Serialize(string topic, GenericRecord data, bool isKey)
        {
            try
            {
                int schemaId;
                global::Avro.Schema writerSchema;
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    // TODO: If any of these caches fills up, this is probably an
                    // indication of misuse of the serializer. Ideally we would do 
                    // something more sophisticated than the below + not allow 
                    // the misuse to keep happening without warning.
                    if (knownSchemas.Count > schemaRegistryClient.MaxCachedSchemas ||
                        registeredSchemas.Count > schemaRegistryClient.MaxCachedSchemas ||
                        schemaIds.Count > schemaRegistryClient.MaxCachedSchemas)
                    {
                        knownSchemas.Clear();
                        registeredSchemas.Clear();
                        schemaIds.Clear();
                    }

                    // Determine a schema string corresponding to the schema object.
                    // TODO: It would be more efficient to use a hash function based
                    // on the instance reference, not the implementation provided by 
                    // Schema.
                    writerSchema = data.Schema;
                    string writerSchemaString = null;
                    if (knownSchemas.ContainsKey(writerSchema))
                    {
                        writerSchemaString = knownSchemas[writerSchema];
                    }
                    else
                    {
                        writerSchemaString = writerSchema.ToString();
                        knownSchemas.Add(writerSchema, writerSchemaString);
                    }

                    // Verify schema compatibility (& register as required) + get the 
                    // id corresponding to the schema.

                    // TODO: Again, the hash functions in use below are potentially 
                    // slow since writerSchemaString is potentially long. It would be
                    // better to use hash functions based on the writerSchemaString 
                    // object reference, not value.

                    string subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic), data.Schema.Fullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : isKey
                            ? schemaRegistryClient.ConstructKeySubjectName(topic, data.Schema.Fullname)
                            : schemaRegistryClient.ConstructValueSubjectName(topic, data.Schema.Fullname);

                    var subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchemaString);
                    if (!registeredSchemas.Contains(subjectSchemaPair))
                    {
                        int newSchemaId;
                        // first usage: register/get schema to check compatibility
                        if (autoRegisterSchema)
                        {
                            newSchemaId = await schemaRegistryClient.RegisterSchemaAsync(subject, writerSchemaString).ConfigureAwait(continueOnCapturedContext: false);
                        }
                        // https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
                        else if (useLatestSchema)
                        {
                            RegisteredSchema regSchema = await schemaRegistryClient.GetLatestSchemaAsync(subject)
                            .ConfigureAwait(continueOnCapturedContext: false);

                            //Do we have an Avro union with schema references
                            if (regSchema.References.Any() && IsUnion(regSchema.SchemaString))
                            {
                                RegisteredSchema registeredRefSchema = null;
                                StringBuilder schemaBuilder = new StringBuilder();
                                schemaBuilder.Append("[");
                                //We need to loop the schema references and perform a schema registry lookup
                                // in order to check compability with referencced schema
                                foreach (var refSchemaString in regSchema.References)
                                {
                                    registeredRefSchema = await schemaRegistryClient.GetRegisteredSchemaAsync(refSchemaString.Subject,
                                          refSchemaString.Version)
                                            .ConfigureAwait(continueOnCapturedContext: false);

                                    Avro.Schema refSchema = Avro.Schema.Parse(registeredRefSchema.SchemaString);

                                    if (refSchema.Tag != Avro.Schema.Type.Record)
                                    {
                                        throw new NotSupportedException("Only union schemas containing references to a record are supported for now");
                                    }

                                    schemaBuilder.Append($"{registeredRefSchema.SchemaString}");
                                    if (regSchema.References.Last() != refSchemaString)
                                    {
                                        schemaBuilder.Append(",");
                                    }
                                }

                                schemaBuilder.Append("]");
                                unionSchemas[writerSchema] = global::Avro.Schema.Parse(schemaBuilder.ToString());
                                newSchemaId = regSchema.Id;
                                // subjectSchemaPair = new KeyValuePair<string, string>(subject, writerSchema.ToString());
                            }
                            else
                            {
                                newSchemaId = await schemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                            }
                        }
                        else
                        {
                            newSchemaId = await schemaRegistryClient.GetSchemaIdAsync(subject, writerSchemaString).ConfigureAwait(continueOnCapturedContext: false);
                        }

                        if (!schemaIds.ContainsKey(writerSchemaString))
                        {
                            schemaIds.Add(writerSchemaString, newSchemaId);
                        }
                        else if (schemaIds[writerSchemaString] != newSchemaId)
                        {
                            schemaIds.Clear();
                            registeredSchemas.Clear();
                            throw new KafkaException(new Error(isKey ? ErrorCode.Local_KeySerialization : ErrorCode.Local_ValueSerialization, $"Duplicate schema registration encountered: Schema ids {schemaIds[writerSchemaString]} and {newSchemaId} are associated with the same schema."));
                        }

                        registeredSchemas.Add(subjectSchemaPair);
                    }

                    schemaId = schemaIds[writerSchemaString];
                }
                finally
                {
                    serializeMutex.Release();
                }

                Avro.Schema unionSchema;
                if (unionSchemas.TryGetValue(writerSchema, out unionSchema))
                {
                  writerSchema = unionSchema;
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);
                    writer.Write(IPAddress.HostToNetworkOrder(schemaId));
                    new GenericWriter<GenericRecord>(writerSchema)
                        .Write(data, new BinaryEncoder(stream));
                    return stream.ToArray();
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        private bool IsUnion(string schemaString)
        {
            return schemaString.StartsWith("[") && schemaString.EndsWith("]");
        }

        /// <inheritdoc/>
        public async Task<byte[]> SerializeAsync(GenericRecord value, SerializationContext context)
        { 
            try
            {
                return await Serialize(context.Topic, value, context.Component == MessageComponentType.Key);
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
