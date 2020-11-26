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

// Disable obsolete warnings. ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes
{
    /// <inheritdoc/>
    public class BmSpecificSerializerImpl<T> : IAsyncSerializer<T>, IAvroSerializerImpl<T>
    {
        internal class SerializerSchemaData
        {
            private string writerSchemaString;
            private global::Avro.Schema writerSchema;

            /// <remarks>
            ///     A given schema is uniquely identified by a schema id, even when
            ///     registered against multiple subjects.
            /// </remarks>
            private int? writerSchemaId;

            private SpecificWriter<T> avroWriter;

            private HashSet<string> subjectsRegistered = new HashSet<string>();

            public HashSet<string> SubjectsRegistered
            {
                get => subjectsRegistered;
                set => subjectsRegistered = value;
            }

            public string WriterSchemaString
            {
                get => writerSchemaString;
                set => writerSchemaString = value;
            }

            public Avro.Schema WriterSchema
            {
                get => writerSchema;
                set => writerSchema = value;
            }

            public int? WriterSchemaId
            {
                get => writerSchemaId;
                set => writerSchemaId = value;
            }

            public SpecificWriter<T> AvroWriter
            {
                get => avroWriter;
                set => avroWriter = value;
            }
        }

        private bool useLatestSchema;
        private ISchemaRegistryClient schemaRegistryClient;
        private bool autoRegisterSchema;
        private int initialBufferSize;
        private SubjectNameStrategyDelegate subjectNameStrategy;

        private Dictionary<Type, SerializerSchemaData> multiSchemaData =
            new Dictionary<Type, SerializerSchemaData>();

        private SerializerSchemaData singleSchemaData = null;

        private SemaphoreSlim serializeMutex = new SemaphoreSlim(1);

        /// <summary>
        ///     Serialize an instance of type <typeparamref name="T"/> to a byte array in Avro format. The serialized
        ///     data is preceded by a "magic byte" (1 byte) and the id of the schema as registered
        ///     in Confluent's Schema Registry (4 bytes, network byte order). This call may block or throw 
        ///     on first use for a particular topic during schema registration.
        /// </summary>
        /// <param name="value">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     A <see cref="System.Threading.Tasks.Task" /> that completes with 
        ///     <paramref name="value" /> serialized as a byte array.
        /// </returns>
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
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

        /// <inheritdoc/>
        public BmSpecificSerializerImpl(
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

            Type writerType = typeof(T);
            if (writerType != typeof(ISpecificRecord))
            {
                singleSchemaData = ExtractSchemaData(writerType);
            }
        }

        private static SerializerSchemaData ExtractSchemaData(Type writerType)
        {
            SerializerSchemaData serializerSchemaData = new SerializerSchemaData();
            if (typeof(ISpecificRecord).IsAssignableFrom(writerType))
            {
                serializerSchemaData.WriterSchema = (global::Avro.Schema)writerType.GetField("_SCHEMA", BindingFlags.Public | BindingFlags.Static).GetValue(null);
            }
            else if (writerType.Equals(typeof(int)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("int");
            }
            else if (writerType.Equals(typeof(bool)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("boolean");
            }
            else if (writerType.Equals(typeof(double)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("double");
            }
            else if (writerType.Equals(typeof(string)))
            {
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET string type, however we don't for consistency
                // with the Java Avro serializer.
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("string");
            }
            else if (writerType.Equals(typeof(float)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("float");
            }
            else if (writerType.Equals(typeof(long)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("long");
            }
            else if (writerType.Equals(typeof(byte[])))
            {
                // Note: It would arguably be better to make this a union with null, to
                // exactly match the .NET byte[] type, however we don't for consistency
                // with the Java Avro serializer.
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("bytes");
            }
            else if (writerType.Equals(typeof(Null)))
            {
                serializerSchemaData.WriterSchema = global::Avro.Schema.Parse("null");
            }
            else
            {
                throw new InvalidOperationException(
                    $"AvroSerializer only accepts type parameters of int, bool, double, string, float, " +
                    "long, byte[], instances of ISpecificRecord and subclasses of SpecificFixed."
                );
            }

            serializerSchemaData.AvroWriter = new SpecificWriter<T>(serializerSchemaData.WriterSchema);
            serializerSchemaData.WriterSchemaString = serializerSchemaData.WriterSchema.ToString();
            return serializerSchemaData;
        }

        /// <inheritdoc/>
        public async Task<byte[]> Serialize(string topic, T data, bool isKey)
        {
            try
            {
                SerializerSchemaData currentSchemaData;
                await serializeMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    if (singleSchemaData == null)
                    {
                        var key = data.GetType();
                        if (!multiSchemaData.TryGetValue(key, out currentSchemaData))
                        {
                            currentSchemaData = ExtractSchemaData(key);
                            multiSchemaData[key] = currentSchemaData;
                        }
                    }
                    else
                    {
                        currentSchemaData = singleSchemaData;
                    }

                    string fullname = null;
                    if (data is ISpecificRecord && ((ISpecificRecord)data).Schema is Avro.RecordSchema)
                    {
                        fullname = ((Avro.RecordSchema)((ISpecificRecord)data).Schema).Fullname;
                    }

                    string subject = this.subjectNameStrategy != null
                        // use the subject name strategy specified in the serializer config if available.
                        ? this.subjectNameStrategy(new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic), fullname)
                        // else fall back to the deprecated config from (or default as currently supplied by) SchemaRegistry.
                        : isKey
                            ? schemaRegistryClient.ConstructKeySubjectName(topic, fullname)
                            : schemaRegistryClient.ConstructValueSubjectName(topic, fullname);

                    if (!currentSchemaData.SubjectsRegistered.Contains(subject))
                    {
                        if (autoRegisterSchema)
                        {
                            currentSchemaData.WriterSchemaId = await schemaRegistryClient
                                        .RegisterSchemaAsync(subject, currentSchemaData.WriterSchemaString)
                                        .ConfigureAwait(continueOnCapturedContext: false);
                        }
                        // https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/
                        else if (useLatestSchema)
                        {
                            RegisteredSchema regSchema = await schemaRegistryClient.GetLatestSchemaAsync(subject)
                            .ConfigureAwait(continueOnCapturedContext: false);

                            int id = -1;
                            //Do we have an Avro union with schema references
                            if (regSchema.References.Any() && IsUnion(regSchema.SchemaString))
                            {
                                RegisteredSchema registeredRefSchema = null;
                                StringBuilder schemaBuilder = new StringBuilder();
                                schemaBuilder.Append("[");

                                foreach (var refSchema in regSchema.References)
                                {
                                    registeredRefSchema = await schemaRegistryClient.GetRegisteredSchemaAsync(refSchema.Subject,
                                          refSchema.Version)
                                            .ConfigureAwait(continueOnCapturedContext: false);

                                    Avro.Schema schema = Avro.Schema.Parse(registeredRefSchema.SchemaString);

                                    if (schema.Tag != Avro.Schema.Type.Record)
                                    {
                                        throw new NotSupportedException("Only union schemas containing references to a record are supported for now");
                                    }

                                    schemaBuilder.Append($"{registeredRefSchema.SchemaString}");
                                    if (regSchema.References.Last() != refSchema)
                                    {
                                        schemaBuilder.Append(",");
                                    }
                                }

                                schemaBuilder.Append("]");
                                currentSchemaData.WriterSchema = global::Avro.Schema.Parse(schemaBuilder.ToString());
                                currentSchemaData.AvroWriter = new SpecificWriter<T>(currentSchemaData.WriterSchema);
                            }
                            else
                            {
                                id = await schemaRegistryClient.GetSchemaIdAsync(subject, currentSchemaData.WriterSchemaString)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                            }

                            currentSchemaData.WriterSchemaId = regSchema.Id;
                        }
                        else
                        {
                            // first usage: register/get schema to check compatibility
                            currentSchemaData.WriterSchemaId = await schemaRegistryClient.GetSchemaIdAsync(subject, currentSchemaData.WriterSchemaString)
                                    .ConfigureAwait(continueOnCapturedContext: false);
                        }

                        currentSchemaData.SubjectsRegistered.Add(subject);
                    }
                }
                finally
                {
                    serializeMutex.Release();
                }

                using (var stream = new MemoryStream(initialBufferSize))
                using (var writer = new BinaryWriter(stream))
                {
                    stream.WriteByte(Constants.MagicByte);

                    writer.Write(IPAddress.HostToNetworkOrder(currentSchemaData.WriterSchemaId.Value));
                    currentSchemaData.AvroWriter.Write(data, new BinaryEncoder(stream));

                    // TODO: maybe change the ISerializer interface so that this copy isn't necessary.
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
    }
}
