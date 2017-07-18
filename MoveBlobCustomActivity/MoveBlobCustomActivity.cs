using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MoveBlobCustomActivityNS
{
    class MoveBlobCustomActivity : CrossAppDomainDotNetActivity<MoveBlobActivityContext>
    {
        protected override MoveBlobActivityContext PreExecute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            LogDataFactoryElements(linkedServices, datasets, activity, logger);

            // Process ADF artifacts up front as these objects are not serializable across app domain boundaries.
            Dataset dataset = datasets.First(ds => ds.Name == activity.Inputs.Single().Name);
            var blobProperties = (AzureBlobDataset) dataset.Properties.TypeProperties;
            LinkedService linkedService =
                linkedServices.First(ls => ls.Name == dataset.Properties.LinkedServiceName);
            var storageProperties = (AzureStorageLinkedService) linkedService.Properties.TypeProperties;
            return new MoveBlobActivityContext
            {
                ConnectionString = storageProperties.ConnectionString,
                FolderPath = blobProperties.FolderPath,
            };

        }

        public override IDictionary<string, string> Execute(
            MoveBlobActivityContext context,
            IActivityLogger logger)
        {
            try
            {
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

        private static void LogBlobDataSetInfo(AzureBlobDataset blobDataset, IActivityLogger logger)
        {
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

            string containerName =
                blobDataset.FolderPath.Substring(0,
                    blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture));
            logger.Write("\nContainer Name {0}", containerName);

            string directoryName =
                blobDataset.FolderPath.Substring(
                    blobDataset.FolderPath.IndexOf("/", StringComparison.InvariantCulture) + 1);
            logger.Write("\nDirectory Name {0}", directoryName);
        }

        private void LogDataFactoryElements(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
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
    }
}
