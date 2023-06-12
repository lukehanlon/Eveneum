using System.Threading;
using System.Threading.Tasks;

namespace Eveneum
{
    public interface IReadStream
    {
        Task<StreamHeaderResponse> ReadHeader(string partitionKey, string streamId, CancellationToken cancellationToken = default);
        Task<StreamResponse> ReadStream(string partitionKey, string streamId, ReadStreamOptions options = default, CancellationToken cancellationToken = default);
    }

    public interface IWriteToStream
    {
        Task<Response> WriteToStream(string partitionKey, string streamId, EventData[] events, ulong? expectedVersion = null, object metadata = null, CancellationToken cancellationToken = default);
    }

    public interface IDeleteStream
    {
        DeleteMode DeleteMode { get; }
        Task<DeleteResponse> DeleteStream(string partitionKey, string streamId, ulong expectedVersion, CancellationToken cancellationToken = default);
    }

    public interface IManageSnapshots
    {
        Task<Response> CreateSnapshot(string partitionKey, string streamId, ulong version, object snapshot, object metadata = null, bool deleteOlderSnapshots = false, CancellationToken cancellationToken = default);
        Task<DeleteResponse> DeleteSnapshots(string partitionKey, string streamId, ulong olderThanVersion, CancellationToken cancellationToken = default);
    }

    public interface IEventStore : IReadStream, IWriteToStream, IDeleteStream, IManageSnapshots
    {
        Task Initialize(CancellationToken cancellationToken = default);
    }
}