using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MoveBlobCustomActivityNS
{
    public class BlobStoreHelper
    {
        private readonly IActivityLogger _logger;
        private readonly string _connectionString;
        private readonly CloudBlobContainer _cloudBlobContainer;
        private readonly CloudBlobClient _cloudBlobClient;

        public BlobStoreHelper(
            IActivityLogger logger,
            string connectionString,
            string containerName)
        {
            _logger = logger;
            _connectionString = connectionString;
            _cloudBlobClient = GetCloudBlobClient(logger);
            _cloudBlobContainer = _cloudBlobClient.GetContainerReference(containerName);
            if (!_cloudBlobContainer.Exists())
                throw new Exception($"Blob container {containerName} doesn't exist");
        }

        public async Task DeleteBlobsAsync(
            string containerName,
            string folderPath)
        {
            var blobs = ListBlobs(containerName, folderPath);

            int count = 0;
            foreach (var blob in blobs)
            {
                _logger.Write($"Deleting {blob.Uri.AbsolutePath}");
                await ((CloudBlockBlob)blob).DeleteIfExistsAsync();
                count++;
            }
            _logger.Write($"Finished deleting {count} blobs");
        }

        public IList<IListBlobItem> ListBlobs(
            string containerName,
            string folderPath)
        {
            try
            {
                _logger.Write("Get IListBlobItems from {0}\\{1}.....", containerName, folderPath);
                var blobItems = _cloudBlobContainer.ListBlobs(prefix:null, useFlatBlobListing:true);
                _logger.Write("Finished getting blobs");

                return new List<IListBlobItem>(blobItems);

            }
            catch (Exception ex)
            {
                var msg = ex.ToString();
                _logger.Write(msg);
                throw;
            }
        }

        // Remember to dispose the stream when done with it.
        public async Task<MemoryStream> GetBlobStreamAsync(Uri blobUri)
        {
            _logger.Write("Getting blob: {0}", blobUri.ToString());
            var blob = await _cloudBlobClient.GetBlobReferenceFromServerAsync(blobUri);
            var stream = new MemoryStream();
            await blob.DownloadToStreamAsync(stream);
            stream.Position = 0;
            return stream;
        }

        private CloudBlobClient GetCloudBlobClient(IActivityLogger logger)
        {
            CloudBlobClient client;
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
                client = storageAccount.CreateCloudBlobClient();
                client.ListContainers(); // Just making sure it works
            }
            catch (Exception ex)
            {
                var msg = "Couldn't create CloudBlobClient";
                logger.Write(msg);
                throw new Exception(msg, ex);
            }

            logger.Write("Created CloudBlobClient");
            return client;
        }
    }
}