using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data.Common;
using Kusto.Ingest.V2;
using Microsoft.VisualBasic;
using System.Collections.Immutable;

namespace TestIngestSdkV2
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var blobsPrefixUrl = Environment.GetEnvironmentVariable("blobsPrefixUrl");
            var blobsSuffix = Environment.GetEnvironmentVariable("blobsSuffix") ?? string.Empty;
            var blobsSas = Environment.GetEnvironmentVariable("blobSas") ?? string.Empty;
            var kustoUri = Environment.GetEnvironmentVariable("kustoUri");

            if (string.IsNullOrEmpty(blobsPrefixUrl))
            {
                Console.WriteLine("Please set 'blobsPrefixUrl' environment variable");
            }
            else if (string.IsNullOrEmpty(blobsSas))
            {
                Console.WriteLine("Please set 'blobsSas' environment variable");
            }
            else if (string.IsNullOrEmpty(kustoUri))
            {
                Console.WriteLine("Please set 'kustoUri' environment variable");
            }
            else
            {
                var credential = new AzureCliCredential();
                var blobUris = await FetchBlobs(credential, blobsPrefixUrl, blobsSuffix, 500);
                (var clusterUri, var database, var table) = AnalyzeKustoUri(kustoUri);
                var ingestClient = QueuedIngestClientBuilder.Create(clusterUri)
                    .WithAuthentication(credential)
                    .Build();
                var properties = new IngestProperties
                {
                    EnableTracking = true
                };

                //await IngestOneBlob(
                //    database, table, ingestClient, blobUris, blobsSas, properties);
                await IngestMultipleBlobs(
                    database, table, ingestClient, blobUris, blobsSas, properties, 20);
            }
        }

        private static async Task IngestMultipleBlobs(
            string database,
            string table,
            IMultiIngest ingestClient,
            IEnumerable<Uri> blobUris,
            string blobsSas,
            IngestProperties properties,
            int blobCount)
        {
            var ingestedBlobs = blobUris
                .Take(blobCount);

            foreach (var uri in ingestedBlobs)
            {
                Console.WriteLine(uri);
            }

            var blobSources = ingestedBlobs
                .Select(u => new BlobSource($"{u}{blobsSas}", DataSourceFormat.parquet));
            var operation = await ingestClient.IngestAsync(blobSources, database, table, properties);
            var operationString = operation.ToJsonString();
            var startTime = DateTime.Now;

            Console.WriteLine($"Operation ID:  {operation.Id}");

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                var summary = await ingestClient.GetOperationSummaryAsync(
                    IngestionOperation.FromJsonString(operationString));

                Console.WriteLine(
                    $"Waiting for ingestion ({summary.Status} ; {DateTime.Now - startTime}):  " +
                    $"{summary.InProgressCount} in progress, " +
                    $"{summary.FailedCount} failed & {summary.SucceededCount} succeeded");

                switch(summary.Status)
                {
                    case IngestStatus.Failed:
                        Console.WriteLine("Ingestion failure!");
                        return;
                    case IngestStatus.Succeeded:
                        Console.WriteLine("Success!");
                        return;
                }
            }
        }

        private static async Task IngestOneBlob(
            string database,
            string table,
            IMultiIngest ingestClient,
            IEnumerable<Uri> blobUris,
            string blobsSas,
            IngestProperties properties)
        {
            var blobSource = new BlobSource(
                $"{blobUris.First()}{blobsSas}",
                DataSourceFormat.parquet);
            var operation = await ingestClient.IngestAsync(blobSource, database, table, properties);
            var operationString = operation.ToJsonString();
            var startTime = DateTime.Now;

            Console.WriteLine($"Operation ID:  {operation.Id}");

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                Console.WriteLine($"Waiting for ingestion...  {DateTime.Now - startTime}");

                var summary = await ingestClient.GetOperationSummaryAsync(
                    IngestionOperation.FromJsonString(operationString));

                if (summary.FailedCount > 0)
                {
                    Console.WriteLine("Ingestion failure!");

                    return;
                }
                else if (summary.SucceededCount > 0)
                {
                    Console.WriteLine("Success!");

                    return;
                }
            }
        }

        private static (Uri clusterUri, string database, string table) AnalyzeKustoUri(string kustoUri)
        {
            var builder = new UriBuilder(kustoUri);
            var path = builder.Path;
            var parts = path.Split('/');
            var database = parts[1];
            var table = parts[2];

            builder.Path = string.Empty;

            return (builder.Uri, database, table);
        }

        private static async Task<IEnumerable<Uri>> FetchBlobs(
            TokenCredential credential,
            string blobsPrefixUrl,
            string blobsSuffix,
            int top)
        {
            var templateBlob = new BlobClient(new Uri(blobsPrefixUrl), credential);
            var containerClient = templateBlob.GetParentBlobContainerClient();
            var blobPrefix = templateBlob.Name;
            var blobUris = ImmutableArray<Uri>.Empty.ToBuilder();

            await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: blobPrefix))
            {
                if (blobItem.Properties.ContentLength > 0
                    && blobItem.Name.EndsWith(blobsSuffix))
                {
                    var uri = new Uri($"{containerClient.Uri}/{blobItem.Name}");

                    blobUris.Add(uri);
                    if (--top <= 0)
                    {
                        break;
                    }
                }
            }

            return blobUris.ToImmutable();
        }
    }
}