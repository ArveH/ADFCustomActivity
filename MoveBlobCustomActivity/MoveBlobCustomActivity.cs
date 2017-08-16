using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MoveBlobCustomActivityNS
{
    class MoveBlobCustomActivity : CrossAppDomainDotNetActivity<MoveBlobActivityContext>
    {
        protected override MoveBlobActivityContext PreExecute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            LogDataFactoryElements(linkedServices, datasets, activity, logger);

            // Getting and logging input information
            var inputDataSet = GetInputDataset(datasets, activity);
            var blobStoreDataset = inputDataSet.Properties.TypeProperties as AzureBlobDataset;
            LogBlobDataSetInfo(blobStoreDataset, logger);
            var inputLinkedService = GetInputLinkedService(linkedServices, inputDataSet);

            // Getting and logging output information
            var outputDataSet = GetOutputDataSet(datasets, activity);
            var adlsDataSet = outputDataSet.Properties.TypeProperties as AzureDataLakeStoreDataset;
            var outputLinkedService = GetOutputLinkedService(linkedServices, outputDataSet);
            var adlsInfo = GetAdlsInfo(outputLinkedService);
            LogAzureDataLakeStoreInfo(adlsDataSet, adlsInfo, logger);

            return new MoveBlobActivityContext
            {
                BlobStorageConnectionString = GetConnectionString(inputLinkedService),
                ContainerName = GetContainerName(blobStoreDataset.FolderPath),
                BlobStorageFolderPath = GetDirectoryName(blobStoreDataset.FolderPath),
                AdlsInfo = adlsInfo,
                AdlsFolderPath = GetAdlsFolderPath(adlsDataSet)
            };
        }

        public override IDictionary<string, string> Execute(
            MoveBlobActivityContext context,
            IActivityLogger logger)
        {
            try
            {
                logger.Write("******** Custom Activity Started ********");
                var blobStoreHelper = new BlobStoreHelper(
                    logger, context.BlobStorageConnectionString, context.ContainerName);
                var adlsHelper = new AdlsHelper(
                    logger, context.AdlsInfo);

                var blobs = blobStoreHelper.ListBlobs(
                    context.ContainerName, context.BlobStorageFolderPath);
                logger.Write("Found {0} blobs on storage account", blobs.Count);

                foreach (var listBlobItem in blobs)
                {
                    try
                    {
                        using (var stream = blobStoreHelper.GetBlobStreamAsync(listBlobItem.Uri).GetAwaiter().GetResult())
                        {
                            var blob = (CloudBlockBlob)listBlobItem; // TODO: Check blob type. It will crash if you try to cast a CloudAppendBlob to CloudBlockBlob
                            var fullPath = context.AdlsFolderPath + "/" + blob.Name;
                            adlsHelper.UploadFromStreamAsync(stream, fullPath).GetAwaiter().GetResult();
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.Write("Moving blob failed with error");
                        logger.Write("Caught exception: ");
                        logger.Write(ex.Message);
                    }
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

        private LinkedService GetOutputLinkedService(
            IEnumerable<LinkedService> linkedServices, 
            Dataset outputDataSet)
        {
            return
                linkedServices
                    .First(ls => ls.Name == outputDataSet.Properties.LinkedServiceName);
        }

        private static Dataset GetOutputDataSet(IEnumerable<Dataset> datasets, Activity activity)
        {
            return datasets.First(ds => ds.Name == activity.Outputs.First().Name);
        }

        private AdlsInfo GetAdlsInfo(LinkedService outputLinkedService)
        {
            var azureDataLakeStoreLinkedService = (AzureDataLakeStoreLinkedService)outputLinkedService.Properties.TypeProperties;
            return new AdlsInfo()
            {
                AzureSubscriptionId = azureDataLakeStoreLinkedService.SubscriptionId,
                AadDomain = azureDataLakeStoreLinkedService.Tenant,
                AadClient = azureDataLakeStoreLinkedService.ServicePrincipalId,
                AadClientSecret = azureDataLakeStoreLinkedService.ServicePrincipalKey,
                AdlsUri = azureDataLakeStoreLinkedService.DataLakeStoreUri,
                AdlsName = azureDataLakeStoreLinkedService.AccountName,
                AdlsResourceGroupName = azureDataLakeStoreLinkedService.ResourceGroupName,
                AdlsAuthorization = azureDataLakeStoreLinkedService.Authorization,
                AdlsSessionId = azureDataLakeStoreLinkedService.SessionId
            };
        }

        private string GetAdlsFolderPath(AzureDataLakeStoreDataset outputDataSet)
        {
            return outputDataSet.FolderPath;
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

        private void LogAzureDataLakeStoreInfo(AzureDataLakeStoreDataset adlsDataSet, AdlsInfo adlsInfo, IActivityLogger logger)
        {
            logger.Write("\n******** Data Lake Store info ********");
            logger.Write("\nName       : " + adlsInfo.AdlsName);
            logger.Write("\nUri        : " + adlsInfo.AdlsUri);
            logger.Write("\nDomain     : " + adlsInfo.AadDomain);
            logger.Write("\nClient     : " + adlsInfo.AadClient);
            logger.Write("\nClientKey  : " + (string.IsNullOrWhiteSpace(adlsInfo.AadClientSecret) ? "<No value!>" : "********"));
            logger.Write("\nAuthorization : " + (string.IsNullOrWhiteSpace(adlsInfo.AdlsAuthorization) ? "<No value!>" : "********"));
            logger.Write("\nResource Group: " + adlsInfo.AdlsResourceGroupName);
            logger.Write("\nSubscription  : " + (string.IsNullOrWhiteSpace(adlsInfo.AzureSubscriptionId) ? "<No value!>" : "********"));
            logger.Write("\nSessionId     : " + adlsInfo.AdlsSessionId);

            logger.Write("\nData folder: " + adlsDataSet.FolderPath);
            logger.Write("\nData format: " + adlsDataSet.Format);

            var partitions = adlsDataSet.PartitionedBy?.Count ?? 0;
            logger.Write($"\nPartitions ({partitions}):");
            for (int i = 0; i < partitions; i++)
            {
                var val = (DateTimePartitionValue)adlsDataSet.PartitionedBy?[i]?.Value;
                
                logger.Write(
                    $"\n\t{adlsDataSet.PartitionedBy?[i]?.Name ?? "null"}: {val?.Date} ({val?.Format})");
            }

            logger.Write("\nBlob file: " + adlsDataSet.FileName);

            if (adlsDataSet.FolderPath.IndexOf("/", StringComparison.InvariantCulture) <= 0)
            {
                throw new Exception($"Can't find container name for dataset '{adlsDataSet.FolderPath}'");
            }

            logger.Write("\nFolder path: {0}", adlsDataSet.FolderPath);
        }
    }
}
