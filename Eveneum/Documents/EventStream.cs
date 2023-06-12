namespace Eveneum.Documents
{
    public class EventStream
    {
        public string StreamId { get; set; }

        public EventData[] Events { get; set; }

        public ulong? ExpectedVersion { get; set; }

        public object Metadata { get; set; }
    }
}