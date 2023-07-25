using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace Eveneum.Documents
{
    public enum DocumentType { Header = 1, Event, Snapshot, EventId }

    public class IdempotenceRecord
    {

        [JsonConverter(typeof(StringEnumConverter))]
        [JsonProperty(PropertyName = "dt")]
        public DocumentType DocumentType { get; }


    }

    public class EveneumDocument
    {
        public EveneumDocument(DocumentType documentType)
        {
            this.DocumentType = documentType;
        }

        public const char Separator = '~';

        [JsonProperty(PropertyName = "pk")]
        public string PartitionKey { get; set; }


        [JsonProperty(PropertyName = "id")]
        public virtual string Id => this.GenerateId();

        [JsonConverter(typeof(StringEnumConverter))]
        [JsonProperty(PropertyName = "dt")]
        public DocumentType DocumentType { get; }

        [JsonProperty(PropertyName = "sid")]
        public string StreamId { get; set; }

        [JsonProperty(PropertyName = "eid")]
        public string EventId { get; set; }

        [JsonProperty(PropertyName = "v")]
        public ulong Version { get; set; }

        [JsonProperty(PropertyName = "mt")]
        public string MetadataType { get; set; }

        [JsonProperty(PropertyName = "m")]
        public JToken Metadata { get; set; }

        [JsonProperty(PropertyName = "bt")]
        public string BodyType { get; set; }

        [JsonProperty(PropertyName = "b")]
        public JToken Body { get; set; }

        [JsonProperty(PropertyName = "so")]
        public decimal SortOrder => this.Version + GetOrderingFraction(this.DocumentType);
        
        [JsonProperty(PropertyName = "d")]
        public bool Deleted { get; set; }

        [JsonProperty(PropertyName = "_etag")]
        public string ETag { get; set; }

        [JsonProperty(PropertyName = "_ts")]
        public string Timestamp { get; set; }

        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? TimeToLive { get; set; }

        internal string GenerateId()
        {
            switch(this.DocumentType)
            {
                case DocumentType.Header:
                    return this.StreamId;
                case DocumentType.Event:
                    return GenerateEventId(this.StreamId, this.EventId);
                case DocumentType.Snapshot:
                    return $"{this.StreamId}{Separator}{this.Version}{Separator}S";
                default:
                    throw new NotSupportedException($"Document type '{this.DocumentType}' is not supported.");
            }
        }

        internal static string GenerateEventId(string streamId, string eventId) => $"{streamId}{Separator}{eventId}";

        internal static decimal GetOrderingFraction(DocumentType documentType)
        {
            switch(documentType)
            {
                case DocumentType.Header:
                    return 0.3M;
                case DocumentType.Snapshot:
                    return 0.2M;
                case DocumentType.Event:
                    return 0.1M;
                default:
                    throw new NotSupportedException($"Document type '{documentType}' is not supported.");
            }
        }
    }
}
