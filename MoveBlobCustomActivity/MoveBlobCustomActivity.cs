using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace MoveBlobCustomActivityNS
{
    class MoveBlobCustomActivity : CrossAppDomainDotNetActivity<MoveBlobActivityContext>
    {
        protected override MoveBlobActivityContext PreExecute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            LogDataFactoryElements(linkedServices, datasets, activity, logger);

            var inputDataSet = GetInputDataset(datasets, activity);
            var blobStoreDataset = inputDataSet.Properties.TypeProperties as AzureBlobDataset;
            LogBlobDataSetInfo(blobStoreDataset, logger);
            var inputLinkedService = GetInputLinkedService(linkedServices, inputDataSet);

            var outputDataSet = GetOutputDataSet(datasets, activity);
            var adlsDataSet = outputDataSet.Properties.TypeProperties as AzureDataLakeStoreDataset;
            LogAzureDataLakeStoreInfo(adlsDataSet, logger);
            var outputLinkedService = GetOutputLinkedService(linkedServices, outputDataSet);

            return new MoveBlobActivityContext
            {
                BlobStorageConnectionString = GetConnectionString(inputLinkedService),
                ContainerName = GetContainerName(blobStoreDataset.FolderPath),
                BlobStorageFolderPath = GetDirectoryName(blobStoreDataset.FolderPath)
            };

        }

        private LinkedService GetOutputLinkedService(IEnumerable<LinkedService> linkedServices, Dataset outputDataSet)
        {
            throw new NotImplementedException();
        }

        public override IDictionary<string, string> Execute(
            MoveBlobActivityContext context,
            IActivityLogger logger)
        {
            try
            {
                logger.Write("******** Custom Activity Ended Successfully ********");
                var blobStoreHelper = new BlobStoreHelper(
                    logger, context.BlobStorageConnectionString, context.ContainerName);
                var blobs = blobStoreHelper.ListBlobs(
                    context.ContainerName, context.BlobStorageFolderPath);
                logger.Write("Found {0} blobs on storage account", blobs.Count);

                foreach (var blob in blobs)
                {
                    //var stream = blobStoreHelper.GetBlobStreamAsync(blob.Uri);

                }

                logger.Write("******** Custom Activity Ended Successfully ********");
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

        private static Dataset GetOutputDataSet(IEnumerable<Dataset> datasets, Activity activity)
        {
            return datasets.First(ds => ds.Name == activity.Outputs.First().Name);
        }

        private static LinkedService GetInputLinkedService(
            IEnumerable<LinkedService> linkedServices,
            Dataset inputDataset)
        {
            return
                linkedServices
                    .First(ls => ls.Name == inputDataset.Properties.LinkedServiceName);
        }

        private static string GetConnectionString(LinkedService inputLinkedService)
        {
            var azureStorageLinkedService = (AzureStorageLinkedService) inputLinkedService.Properties.TypeProperties;
            return azureStorageLinkedService.ConnectionString;
        }

        private static string GetDirectoryName(string folderPath)
        {
            return folderPath.Substring(
                folderPath.IndexOf("/", StringComparison.InvariantCulture) + 1);
        }

        private static string GetContainerName(string folderPath)
        {
            return folderPath.Substring(0,
                folderPath.IndexOf("/", StringComparison.InvariantCulture));
        }

        private void LogDataFactoryElements(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            logger.Write("\n******** Data Factory info ********");

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

        private static void LogBlobDataSetInfo(AzureBlobDataset blobDataset, IActivityLogger logger)
        {
            logger.Write("\n******** Blob Storage info ********");
            logger.Write("\nBlob folder: " + blobDataset.FolderPath);
            logger.Write("\nBlob format: " + blobDataset.Format);

            var partitions = blobDataset.PartitionedBy?.Count ?? 0;
            logger.Write($"\nPartitions ({partitions}):");
            for (int i = 0; i < partitions; i++)
            {
                logger.Write(
                    $"\n\t{blobDataset.PartitionedBy?[i].Name ?? "null"}: {blobDataset.PartitionedBy?[i]?.Value}");
            }

            logger.Write("\nBlob file: " + blobDataset.FileName);

            if (blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) <= 0)
            {
                throw new Exception($"Can't find container name for dataset '{blobDataset.FolderPath}'");
            }

            logger.Write("\nContainer Name: {0}", GetContainerName(blobDataset.FolderPath));
            logger.Write("\nDirectory Name: {0}", GetDirectoryName(blobDataset.FolderPath));
        }

        private void LogAzureDataLakeStoreInfo(AzureDataLakeStoreDataset adlsDataSet, IActivityLogger logger)
        {
            logger.Write("\n******** Data Lake Store info ********");
            logger.Write("\nData folder: " + adlsDataSet.FolderPath);
            logger.Write("\nData format: " + adlsDataSet.Format);

            var partitions = adlsDataSet.PartitionedBy?.Count ?? 0;
            logger.Write($"\nPartitions ({partitions}):");
            for (int i = 0; i < partitions; i++)
            {
                logger.Write(
                    $"\n\t{adlsDataSet.PartitionedBy?[i].Name ?? "null"}: {adlsDataSet.PartitionedBy?[i]?.Value}");
            }

            logger.Write("\nBlob file: " + adlsDataSet.FileName);

            if (adlsDataSet.FolderPath.IndexOf("/", StringComparison.InvariantCulture) <= 0)
            {
                throw new Exception($"Can't find container name for dataset '{adlsDataSet.FolderPath}'");
            }

            logger.Write("\nContainer Name: {0}", GetContainerName(adlsDataSet.FolderPath));
            logger.Write("\nDirectory Name: {0}", GetDirectoryName(adlsDataSet.FolderPath));
        }
    }
}
