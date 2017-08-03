using System;
using System.Collections.Generic;
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
        private readonly CloudBlobClient _cloudBlobClient;

        public BlobStoreHelper(
            IActivityLogger logger,
            string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
            _cloudBlobClient = GetCloudBlobClient(logger);
        }

        public async Task DeleteBlobsAsync(
            string containerName,
            string folderPath)
        {
            var blobs = await GetBlobsAsync(containerName, folderPath);

            int count = 0;
            foreach (var blob in blobs)
            {
                _logger.Write($"Deleting {blob.Uri.AbsolutePath}");
                await ((CloudBlockBlob)blob).DeleteIfExistsAsync();
                count++;
            }
            _logger.Write($"Finished deleting {count} blobs");
        }

        public async Task<IList<IListBlobItem>> GetBlobsAsync(
            string containerName,
            string folderPath)
        {
            try
            {
                var container = _cloudBlobClient.GetContainerReference(containerName);
                if (!await container.ExistsAsync())
                    throw new Exception($"Blob container {containerName} doesn't exist");

                _logger.Write("Get IListBlobItems from {0}\\{1}.....", containerName, folderPath);
                var blobItems = container.ListBlobs();
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