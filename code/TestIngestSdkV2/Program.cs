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
            var kustoUri = Environment.GetEnvironmentVariable("kustoUri");

            if (string.IsNullOrEmpty(blobsPrefixUrl))
            {
                Console.WriteLine("Please set 'blobsPrefixUrl' environment variable");
            }
            else if (string.IsNullOrEmpty(kustoUri))
            {
                Console.WriteLine("Please set 'kustoUri' environment variable");
            }
            else
            {
                var credential = new AzureCliCredential();
                (var clusterUri, var database, var table) = AnalyzeKustoUri(kustoUri);
                var ingestClient = QueuedIngestClientBuilder.Create(clusterUri)
                    .WithAuthentication(credential)
                    .Build();
                var ingestionProperties = new KustoIngestionProperties(database, table)
                {
                    Format = DataSourceFormat.csv
                };
                ingestClient.IngestFromStorage();

                var blobUris = await FetchBlobs(credential, blobsPrefixUrl, blobsSuffix, 1);
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