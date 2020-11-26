// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.7.7.5
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace Confluent.Kafka.Examples.AvroSpecific
{
    using global::Avro;
    using global::Avro.Specific;

    public partial class EventA : ISpecificRecord
    {
        public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"EventA\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific\",\"fields\":[{\"na" +
            "me\":\"EventType\",\"type\":\"string\"},{\"name\":\"EventId\",\"type\":\"string\"},{\"name\":\"Occ" +
            "uredOn\",\"type\":\"long\"},{\"name\":\"A\",\"type\":\"string\"}]}");
        private string _EventType;
        private string _EventId;
        private long _OccuredOn;
        private string _A;
        public virtual Schema Schema
        {
            get
            {
                return EventA._SCHEMA;
            }
        }
        public string EventType
        {
            get
            {
                return this._EventType;
            }
            set
            {
                this._EventType = value;
            }
        }
        public string EventId
        {
            get
            {
                return this._EventId;
            }
            set
            {
                this._EventId = value;
            }
        }
        public long OccuredOn
        {
            get
            {
                return this._OccuredOn;
            }
            set
            {
                this._OccuredOn = value;
            }
        }
        public string A
        {
            get
            {
                return this._A;
            }
            set
            {
                this._A = value;
            }
        }
        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.EventType;
                case 1: return this.EventId;
                case 2: return this.OccuredOn;
                case 3: return this.A;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }
        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: this.EventType = (System.String)fieldValue; break;
                case 1: this.EventId = (System.String)fieldValue; break;
                case 2: this.OccuredOn = (System.Int64)fieldValue; break;
                case 3: this.A = (System.String)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }
    }
}