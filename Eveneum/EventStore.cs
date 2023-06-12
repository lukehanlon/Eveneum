using Eveneum.Advanced;
using Eveneum.Documents;
using Eveneum.Serialization;
using Eveneum.StoredProcedures;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Eveneum
{
    public class EventStore : IEventStore, IAdvancedEventStore
    {
        public readonly CosmosClient Client;
        public readonly Database Database;
        public readonly Container Container;

        public DeleteMode DeleteMode { get; }
        public TimeSpan StreamTimeToLiveAfterDelete { get; }
        public byte BatchSize { get; }
        public int QueryMaxItemCount { get; }
        public EveneumDocumentSerializer Serializer { get; }

        private const string BulkDeleteStoredProc = "Eveneum.BulkDelete";

        public EventStore(CosmosClient client, string database, string container, EventStoreOptions options = null)
        {
            this.Client = client ?? throw new ArgumentNullException(nameof(client));
            this.Database = this.Client.GetDatabase(database ?? throw new ArgumentNullException(nameof(database)));
            this.Container = this.Database.GetContainer(container ?? throw new ArgumentNullException(nameof(container)));

            options = options ?? new EventStoreOptions();

            this.DeleteMode = options.DeleteMode;
            this.StreamTimeToLiveAfterDelete = options.StreamTimeToLiveAfterDelete;
            this.BatchSize = Math.Min(options.BatchSize, (byte)100); // Maximum batch size supported by CosmosDB
            this.QueryMaxItemCount = options.QueryMaxItemCount;
            this.Serializer = new EveneumDocumentSerializer(options.JsonSerializer, options.TypeProvider, options.IgnoreMissingTypes);
        }

        public async Task Initialize(CancellationToken cancellationToken = default)
        {
            await CreateStoredProcedure(BulkDeleteStoredProc, "BulkDelete", cancellationToken);
        }

        public Task<StreamResponse> ReadStreamAsOfVersion(string partitionKey, string streamId, ulong version, CancellationToken cancellationToken = default) =>
            ReadStream(partitionKey, streamId, new ReadStreamOptions { FromVersion = null, ToVersion = version, IgnoreSnapshots = false, MaxItemCount = 100 }, cancellationToken);
        public Task<StreamResponse> ReadStreamFromVersion(string partitionKey, string streamId, ulong version, CancellationToken cancellationToken = default) =>
            ReadStream(partitionKey, streamId, new ReadStreamOptions { FromVersion = version, ToVersion = null, IgnoreSnapshots = true, MaxItemCount = null }, cancellationToken);
        public Task<StreamResponse> ReadStreamIgnoringSnapshots(string partitionKey, string streamId, CancellationToken cancellationToken = default) =>
            ReadStream(partitionKey, streamId, new ReadStreamOptions { FromVersion = null, ToVersion = null, IgnoreSnapshots = true, MaxItemCount = null }, cancellationToken);

        public Task<StreamResponse> ReadStream(string partitionKey, string streamId, ReadStreamOptions options = null, CancellationToken cancellationToken = default)
        {
            options = options ?? new ReadStreamOptions();

            var maxItemCount = options.MaxItemCount ?? QueryMaxItemCount;

            var whereTerms = new List<string>();

            if (options.IgnoreSnapshots)
                whereTerms.Add($"x.dt <> '{nameof(DocumentType.Snapshot)}'");

            if (options.FromVersion.HasValue)
                whereTerms.Add($"(x.v >= {options.FromVersion.Value} OR x.dt = '{nameof(DocumentType.Header)}')");

            if (options.ToVersion.HasValue)
                whereTerms.Add($"(x.v <= {options.ToVersion.Value} OR x.dt = '{nameof(DocumentType.Header)}')");

            var selectClause = "SELECT * FROM x";
            var whereClause = whereTerms.Count > 0 
                ? $"WHERE {string.Join(" AND ", whereTerms)}" 
                : string.Empty;
            var orderByClause = $"ORDER BY x.{nameof(EveneumDocument.SortOrder)} DESC";

            var query = $"{selectClause} {whereClause} {orderByClause}";

            return ReadStream(partitionKey, streamId, query, maxItemCount, cancellationToken);
        }

        private async Task<StreamResponse> ReadStream(string partitionKey, string streamId, string sql, int maxItemCount, CancellationToken cancellationToken)
        {
            if (streamId == null)
                throw new ArgumentNullException(nameof(streamId));

            var iterator = this.Container.GetItemQueryIterator<EveneumDocument>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(partitionKey), MaxItemCount = maxItemCount });

            var documents = new List<EveneumDocument>();
            var finishLoading = false;
            double requestCharge = 0;

            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync(cancellationToken);

                requestCharge += page.RequestCharge;

                foreach (var eveneumDoc in page)
                {
                    if (eveneumDoc.DocumentType == DocumentType.Header && eveneumDoc.Deleted)
                        return new StreamResponse(null, true, requestCharge);

                    if (eveneumDoc.Deleted)
                        continue;

                    documents.Add(eveneumDoc);

                    if (eveneumDoc.DocumentType == DocumentType.Snapshot)
                    {
                        finishLoading = true;
                        break;
                    }
                }

                if (finishLoading)
                    break;
            }

            if (documents.Count == 0)
                return new StreamResponse(null, false, requestCharge);

            var headerDocument = documents.First(x => x.DocumentType == DocumentType.Header);

            try
            {
                var events = documents.Where(x => x.DocumentType == DocumentType.Event).Select(this.Serializer.DeserializeEvent).Reverse().ToArray();
                var snapshot = documents.Where(x => x.DocumentType == DocumentType.Snapshot).Select(this.Serializer.DeserializeSnapshot).Cast<Snapshot?>().FirstOrDefault();
                var metadata = this.Serializer.DeserializeObject(headerDocument.MetadataType, headerDocument.Metadata);

                return new StreamResponse(new Stream(streamId, headerDocument.Version, metadata, events, snapshot), false, requestCharge);
            }
            catch (TypeNotFoundException ex)
            {
                throw new StreamDeserializationException(streamId, requestCharge, ex.Type, ex);
            }
            catch (JsonDeserializationException ex)
            {
                throw new StreamDeserializationException(streamId, requestCharge, ex.Type, ex);
            }
        }


        public async Task<Response> WriteToPartition(string partitionKey,
            EventStream[] eventStreams, CancellationToken cancellationToken = default)
        {
            var transaction = this.Container.CreateTransactionalBatch(new PartitionKey(partitionKey));
            double requestCharge = 0;

            var maximumItems = eventStreams.Sum(s => (s.Events.Length * 2) + 1);

            if (maximumItems > 100)
            {
                throw new NotSupportedException(
                    $"The maximum number of events that can be written has been exceeded. Attempted to write {maximumItems} events. The maximum number is 49.");
            }

            foreach (var eventStream in eventStreams)
            {
                // Existing stream
                if (eventStream.ExpectedVersion.HasValue)
                {
                    var headerResponse = await this.ReadHeaderDocument(partitionKey, eventStream.StreamId,  cancellationToken);

                    var header = headerResponse.Document;
                    requestCharge += headerResponse.RequestCharge;

                    if (header.Deleted)
                        throw new StreamDeletedException(eventStream.StreamId, requestCharge);

                    if (header.Version != eventStream.ExpectedVersion)
                        throw new OptimisticConcurrencyException(eventStream.StreamId, requestCharge, eventStream.ExpectedVersion.Value,
                            header.Version);

                    header.Version += (ulong)eventStream.Events.Length;

                    this.Serializer.SerializeHeaderMetadata(header, eventStream.Metadata);

                    transaction.ReplaceItem(header.Id, header,
                        new TransactionalBatchItemRequestOptions { IfMatchEtag = header.ETag });
                }
                else
                {
                    var header = new EveneumDocument(DocumentType.Header)
                    { StreamId = eventStream.StreamId, Version = (ulong)eventStream.Events.Length, PartitionKey = partitionKey };

                    this.Serializer.SerializeHeaderMetadata(header, eventStream.Metadata);

                    transaction.CreateItem(header);
                }

                var events = eventStream.Events.Select(@event => this.Serializer.SerializeEvent(@event, eventStream.StreamId, partitionKey));
                foreach (var document in events)
                    transaction.CreateItem(document);
                var idempotenceRecord = eventStream.Events.Select(@event => this.Serializer.SerializeEvent(@event, eventStream.StreamId, partitionKey));
                foreach (var document in events)
                    transaction.CreateItem(document);
            }


            var response = await transaction.ExecuteAsync(cancellationToken);
            requestCharge += response.RequestCharge;

            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                throw new StreamAlreadyExistsException(string.Join(",", eventStreams.Select(s => s.StreamId)), requestCharge);
            else if (!response.IsSuccessStatusCode)
                throw new WriteException(string.Join(",", eventStreams.Select(s => s.StreamId)), requestCharge, response.ErrorMessage, response.StatusCode);

            return new Response(requestCharge);
        }

        public async Task<Response> WriteToStream(string partitionKey, string streamId, EventData[] events, ulong? expectedVersion = null, object metadata = null, CancellationToken cancellationToken = default)
        {
            var transaction = this.Container.CreateTransactionalBatch(new PartitionKey(streamId));
            double requestCharge = 0;

            // Existing stream
            if (expectedVersion.HasValue)
            {
                var headerResponse = await this.ReadHeaderDocument(partitionKey, streamId, cancellationToken);

                var header = headerResponse.Document;
                requestCharge += headerResponse.RequestCharge;

                if (header.Deleted)
                    throw new StreamDeletedException(streamId, requestCharge);

                if (header.Version != expectedVersion)
                    throw new OptimisticConcurrencyException(streamId, requestCharge, expectedVersion.Value, header.Version);

                header.Version += (ulong)events.Length;

                this.Serializer.SerializeHeaderMetadata(header, metadata);

                transaction.ReplaceItem(header.Id, header, new TransactionalBatchItemRequestOptions { IfMatchEtag = header.ETag });
            }
            else
            {
                var header = new EveneumDocument(DocumentType.Header) { StreamId = streamId, Version = (ulong)events.Length };

                this.Serializer.SerializeHeaderMetadata(header, metadata);

                transaction.CreateItem(header);
            }

            var firstBatch = events.Take(this.BatchSize - 1).Select(@event => this.Serializer.SerializeEvent(@event, streamId, partitionKey));
            foreach (var document in firstBatch)
                transaction.CreateItem(document);

            var response = await transaction.ExecuteAsync(cancellationToken);
            requestCharge += response.RequestCharge;

            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                throw new StreamAlreadyExistsException(streamId, requestCharge);
            else if (!response.IsSuccessStatusCode)
                throw new WriteException(streamId, requestCharge, response.ErrorMessage, response.StatusCode);

            foreach (var batch in events.Skip(this.BatchSize - 1).Select(@event => this.Serializer.SerializeEvent(@event, streamId, partitionKey)).Batch(this.BatchSize))
            {
                transaction = this.Container.CreateTransactionalBatch(new PartitionKey(streamId));

                foreach (var document in batch)
                    transaction.CreateItem(document);

                response = await transaction.ExecuteAsync(cancellationToken);
                requestCharge += response.RequestCharge;

                if (!response.IsSuccessStatusCode)
                    throw new WriteException(streamId, requestCharge, response.ErrorMessage, response.StatusCode);
            }

            return new Response(requestCharge);
        }

        public async Task<DeleteResponse> DeleteStream(string partitionKey, string streamId, ulong expectedVersion, CancellationToken cancellationToken = default)
        {
            var partitionKeyObject = new PartitionKey(partitionKey);
            var headerResponse = await this.ReadHeaderDocument(partitionKey, streamId, cancellationToken);

            var existingHeader = headerResponse.Document;
            var requestCharge = headerResponse.RequestCharge;

            if (existingHeader == null)
                throw new StreamNotFoundException(streamId, requestCharge);

            if (existingHeader.Deleted)
                throw new StreamDeletedException(streamId, requestCharge);

            if (existingHeader.Version != expectedVersion)
                throw new OptimisticConcurrencyException(streamId, requestCharge, expectedVersion, existingHeader.Version);

            ulong deletedDocuments = 0;

            StoredProcedureExecuteResponse<BulkDeleteResponse> response;
            var query = $"SELECT * FROM c";

            var useSoftDeleteMode = (this.DeleteMode == DeleteMode.SoftDelete) || (this.DeleteMode == DeleteMode.TtlDelete);

            if (useSoftDeleteMode)
                query += " WHERE c.d = false";

            do
            {
                var ttl = this.DeleteMode == DeleteMode.TtlDelete ? StreamTimeToLiveAfterDelete.TotalSeconds  : -1;                
                response = await this.Container.Scripts.ExecuteStoredProcedureAsync<BulkDeleteResponse>(BulkDeleteStoredProc, partitionKeyObject, new object[] { query, useSoftDeleteMode, ttl}, cancellationToken: cancellationToken);

                requestCharge += response.RequestCharge;
                deletedDocuments += response.Resource.Deleted;
            }
            while (response.Resource.Continuation);

            return new DeleteResponse(deletedDocuments, requestCharge);
        }

        public async Task<Response> CreateSnapshot(string partitionKey, string streamId, ulong version, object snapshot, object metadata = null, bool deleteOlderSnapshots = false, CancellationToken cancellationToken = default)
        {
            var headerResponse = await this.ReadHeaderDocument(partitionKey,streamId, cancellationToken);

            var header = headerResponse.Document;
            var requestCharge = headerResponse.RequestCharge;

            if (header == null)
                throw new StreamNotFoundException(streamId, requestCharge);

            if (header.Deleted)
                throw new StreamDeletedException(streamId, requestCharge);

            if (header.Version < version)
                throw new OptimisticConcurrencyException(streamId, requestCharge, version, header.Version);

            var document = this.Serializer.SerializeSnapshot(snapshot, metadata, version, streamId);

            var response = await this.Container.UpsertItemAsync(document, new PartitionKey(streamId), cancellationToken: cancellationToken);

            requestCharge += response.RequestCharge;

            if (deleteOlderSnapshots)
            {
                var deleteResponse = await this.DeleteSnapshots(partitionKey, streamId, version, cancellationToken);

                requestCharge += deleteResponse.RequestCharge;
            }

            return new Response(requestCharge);
        }

        public async Task<DeleteResponse> DeleteSnapshots(string partitionKey, string streamId, ulong olderThanVersion, CancellationToken cancellationToken = default)
        {
            var headerResponse = await this.ReadHeader(partitionKey, streamId, cancellationToken);

            var requestCharge = headerResponse.RequestCharge;
            ulong deletedSnapshots = 0;
            StoredProcedureExecuteResponse<BulkDeleteResponse> response;
            var query = $"SELECT * FROM c WHERE c.dt = 'Snapshot' AND c.Version < {olderThanVersion}";

            if (this.DeleteMode == DeleteMode.SoftDelete)
                query += " and c.d = false";

            do
            {
                response = await this.Container.Scripts.ExecuteStoredProcedureAsync<BulkDeleteResponse>(BulkDeleteStoredProc, new PartitionKey(streamId), new object[] { query, this.DeleteMode == DeleteMode.SoftDelete }, cancellationToken: cancellationToken);
                requestCharge += response.RequestCharge;
                deletedSnapshots += response.Resource.Deleted;
            }
            while (response.Resource.Continuation);

            return new DeleteResponse(deletedSnapshots, requestCharge);
        }

        public Task<Response> LoadAllEvents(Func<IReadOnlyCollection<EventData>, Task> callback, CancellationToken cancellationToken = default) =>
            this.LoadEvents($"SELECT * FROM c WHERE c.dt = '{nameof(DocumentType.Event)}'", callback, cancellationToken);

        public Task<Response> LoadEvents(string query, Func<IReadOnlyCollection<EventData>, Task> callback, CancellationToken cancellationToken = default)
            => this.LoadEvents(new QueryDefinition(query), callback, cancellationToken);

        public Task<Response> LoadEvents(QueryDefinition query, Func<IReadOnlyCollection<EventData>, Task> callback, CancellationToken cancellationToken = default)
            => LoadDocuments(query, response => callback(response.Where(x => x.DocumentType == DocumentType.Event).Select(this.Serializer.DeserializeEvent).ToList()), cancellationToken);

        public Task<Response> LoadStreamHeaders(string query, Func<IReadOnlyCollection<StreamHeader>, Task> callback, CancellationToken cancellationToken = default)
            => this.LoadStreamHeaders(new QueryDefinition(query), callback, cancellationToken);

        public Task<Response> LoadStreamHeaders(QueryDefinition query, Func<IReadOnlyCollection<StreamHeader>, Task> callback, CancellationToken cancellationToken = default)
            => LoadDocuments(query, response => callback(response.Where(x => x.DocumentType == DocumentType.Header).Select(x => new StreamHeader(x.StreamId, x.Version, this.Serializer.DeserializeObject(x.MetadataType, x.Metadata), x.Deleted)).ToList()), cancellationToken);

        public async Task<Response> ReplaceEvent(string partitionKey, EventData newEvent, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await this.Container.ReplaceItemAsync(this.Serializer.SerializeEvent(newEvent, newEvent.StreamId, partitionKey), EveneumDocument.GenerateEventId(newEvent.StreamId, newEvent.Version), new PartitionKey(partitionKey), cancellationToken: cancellationToken);

                return new Response(response.RequestCharge);
            }
            catch (CosmosException ex)
            {
                throw new WriteException(newEvent.StreamId, ex.RequestCharge, ex.Message, ex.StatusCode, ex);
            }
        }

        public async Task<StreamHeaderResponse> ReadHeader(string partitionKey, string streamId, CancellationToken cancellationToken = default)
        {
            var result = await this.ReadHeaderDocument(partitionKey, streamId, cancellationToken);

            return new StreamHeaderResponse(new StreamHeader(streamId, result.Document.Version, this.Serializer.DeserializeObject(result.Document.MetadataType, result.Document.Metadata), result.Document.Deleted), result.RequestCharge);
        }

        private async Task<Response> LoadDocuments(QueryDefinition query, Func<FeedResponse<EveneumDocument>, Task> callback, CancellationToken cancellationToken = default)
        {
            var iterator = this.Container.GetItemQueryIterator<EveneumDocument>(query, requestOptions: new QueryRequestOptions { MaxItemCount = this.QueryMaxItemCount });

            double requestCharge = 0;
            var callbackProcessing = Task.CompletedTask;

            do
            {
                var response = await iterator.ReadNextAsync(cancellationToken);

                requestCharge += response.RequestCharge;

                await callbackProcessing;

                callbackProcessing = callback(response);
            }
            while (iterator.HasMoreResults);

            await callbackProcessing;

            return new Response(requestCharge);
        }

        private async Task<DocumentResponse> ReadHeaderDocument(string partitionKey, string streamId, CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await this.Container.ReadItemAsync<EveneumDocument>(streamId, new PartitionKey(streamId), cancellationToken: cancellationToken);

                return new DocumentResponse(result.Resource, result.RequestCharge);
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new StreamNotFoundException(streamId, ex.RequestCharge, ex);
            }
        }

        private async Task CreateStoredProcedure(string procedureId, string procedureFileName, CancellationToken cancellationToken = default)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(typeof(EventStore), $"StoredProcedures.{procedureFileName}.js"))
            using (var reader = new StreamReader(stream))
            {
                var properties = new StoredProcedureProperties
                {
                    Id = procedureId,
                    Body = await reader.ReadToEndAsync()
                };

                try
                {
                    await this.Container.Scripts.ReadStoredProcedureAsync(procedureId, cancellationToken: cancellationToken);
                    await this.Container.Scripts.ReplaceStoredProcedureAsync(properties, cancellationToken: cancellationToken);
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await this.Container.Scripts.CreateStoredProcedureAsync(properties, cancellationToken: cancellationToken);
                }
            }
        }
    }
}
