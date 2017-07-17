using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MoveBlobCustomActivityNS
{
    public class MoveBlobCustomActivity: IDotNetActivity
    {
        public IDictionary<string, string> Execute(
            IEnumerable<LinkedService> linkedServices, 
            IEnumerable<Dataset> datasets, 
            Activity activity, 
            IActivityLogger logger)
        {
            try
            {
                LogDataFactoryElements(linkedServices, datasets, activity, logger);

                var inputDataSet = GetInputDataset(datasets, activity);
                var inputLinkedService = GetInputLinkedService(linkedServices, inputDataSet);

                CloudBlobClient blobStorageClient = GetBlobStorageClient(
                    inputLinkedService,
                    logger);

                DeleteBlobs(
                    blobStorageClient,
                    inputDataSet.Properties.TypeProperties as AzureBlobDataset,
                    logger);

                logger.Write("Custom Activity Ended Successfully.");
            }
            catch (Exception e)
            {
                logger.Write("Custom Activity Failed with error.");
                logger.Write("Caught exception: ");
                logger.Write(e.Message);
                throw new Exception(e.Message);
            }

            return new Dictionary<string, string>();
        }

        private static Dataset GetInputDataset(IEnumerable<Dataset> datasets, Activity activity)
        {
            return datasets.First(ds => ds.Name == activity.Inputs.First().Name);
        }

        private static AzureStorageLinkedService GetInputLinkedService(
            IEnumerable<LinkedService> linkedServices,
            Dataset inputDataset)
        {
            return
                linkedServices
                    .First(ls => ls.Name == inputDataset.Properties.LinkedServiceName)
                    .Properties
                    .TypeProperties as AzureStorageLinkedService;
        }

        private static CloudBlobClient GetBlobStorageClient(
            AzureStorageLinkedService inputLinkedService,
            IActivityLogger logger)
        {
            if (string.IsNullOrWhiteSpace(inputLinkedService.ConnectionString))
            {
                var msg = "Connection string is empty";
                logger.Write(msg);
                throw new Exception(msg);
            }

            CloudBlobClient client;
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(inputLinkedService.ConnectionString);
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

        private static void LogDataFactoryElements(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity,
            IActivityLogger logger)
        {
            DotNetActivity dotNetActivity = (DotNetActivity) activity.TypeProperties;

            IDictionary<string, string> extendedProperties = dotNetActivity.ExtendedProperties;
            logger.Write("\nLogging extended properties if any...");
            foreach (KeyValuePair<string, string> entry in extendedProperties)
            {
                logger.Write("\n\t<key:{0}> <value:{1}>", entry.Key, entry.Value);
            }

            logger.Write("\nAll Linked Services(s) Below ");
            foreach (LinkedService ls in linkedServices)
                logger.Write("\n\tLinked Service: " + ls.Name);

            logger.Write("\nAll Dataset(s) Below ");
            foreach (Dataset ds in datasets)
                logger.Write("\n\tDataset: " + ds.Name);

            foreach (string name in activity.Inputs.Select(i => i.Name))
            {
                logger.Write("\nInput Dataset: " + name);
            }

            foreach (string name in activity.Outputs.Select(i => i.Name))
            {
                logger.Write("\nOutput Dataset: " + name);
            }
        }

        private void DeleteBlobs(
            CloudBlobClient blobStorageClient, 
            AzureBlobDataset blobDataset, 
            IActivityLogger logger)
        {
            LogBlobDataSetInfo(blobDataset, logger);

            var folderPath = blobDataset.FolderPath;

            var blobs = blobStorageClient.ListBlobs(folderPath, true).ToList();
            logger.Write($"Found {blobs.Count}");
        }

        private static void LogBlobDataSetInfo(AzureBlobDataset blobDataset, IActivityLogger logger)
        {
            logger.Write("\nBlob folder: " + blobDataset.FolderPath);
            logger.Write("\nBlob format: " + blobDataset.Format);

            var partitions = blobDataset.PartitionedBy?.Count ?? 0;
            logger.Write($"\nPartitions ({partitions}):");
            for (int i = 0; i < partitions; i++)
            {
                logger.Write($"\n\t{blobDataset.PartitionedBy?[i].Name ?? "null"}: {blobDataset.PartitionedBy?[i]?.Value}");
            }

            logger.Write("\nBlob file: " + blobDataset.FileName);

            if (blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) <= 0)
            {
                throw new Exception($"Can't find container name for dataset '{blobDataset.FolderPath}'");
            }

            string containerName =
                blobDataset.FolderPath.Substring(0,
                    blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture));
            logger.Write("\nContainer Name {0}", containerName);

            string directoryName =
                blobDataset.FolderPath.Substring(
                    blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) + 1);
            logger.Write("\nDirectory Name {0}", directoryName);
        }

        public void DeleteBlobFileFolder(
            IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets,
            List<string> dataSetsToDelete,
            IActivityLogger logger)
        {
            foreach (string strInputToDelete in dataSetsToDelete)
            {
                Dataset inputDataset = datasets.First(ds => ds.Name.Equals(strInputToDelete));
                AzureBlobDataset blobDataset = inputDataset.Properties.TypeProperties as AzureBlobDataset;
                logger.Write("\nBlob folder: " + blobDataset.FolderPath);
                logger.Write("\nBlob format: " + blobDataset.Format);

                logger.Write("\nPartitions (if any):");
                foreach (var partition in blobDataset.PartitionedBy)
                {
                    logger.Write($"\n\t{partition.Name}: {partition.Value}");
                }
                logger.Write("\nBlob file: " + blobDataset.FileName);

                AzureStorageLinkedService inputLinkedService = linkedServices.First(ls =>
                    ls.Name == inputDataset.Properties.LinkedServiceName).Properties.TypeProperties as AzureStorageLinkedService;

                // create storage client for input. Pass the connection string.
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(inputLinkedService.ConnectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();

                // find blob to delete and delete if exists.
                Uri blobUri = new Uri(storageAccount.BlobEndpoint, blobDataset.FolderPath + blobDataset.FileName);
                CloudBlockBlob blob = new CloudBlockBlob(blobUri, storageAccount.Credentials);
                logger.Write("Blob Uri: {0}", blobUri.AbsoluteUri);
                logger.Write("Blob exists: {0}", blob.Exists());
                blob.DeleteIfExists();
                logger.Write("Deleted blob: {0}", blobUri.AbsoluteUri);

                // Ensure the container is exist.
                if (blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) > 0)
                {
                    string containerName = blobDataset.FolderPath.Substring(0, blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture));
                    logger.Write("Container Name {0}", containerName);

                    string directoryName = blobDataset.FolderPath.Substring(blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) + 1);
                    logger.Write("Directory Name {0}", directoryName);

                    var blobContainer = client.GetContainerReference(containerName);
                    blobContainer.CreateIfNotExists();

                    foreach (IListBlobItem item in blobContainer.ListBlobs(directoryName, true))
                    {
                        logger.Write("Blob Uri: {0} ", item.Uri.AbsoluteUri);

                        if (item is CloudBlockBlob || item.GetType().BaseType == typeof(CloudBlockBlob))
                        {
                            CloudBlockBlob subBlob = new CloudBlockBlob(item.Uri, storageAccount.Credentials);
                            logger.Write("Blob exists: {0}", subBlob.Exists());
                            subBlob.DeleteIfExists();
                            logger.Write("Deleted blob {0}", item.Uri.AbsoluteUri);
                        }
                    }
                }
            }
        }
    }
}
