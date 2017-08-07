using System;

namespace MoveBlobCustomActivityNS
{
    [Serializable]
    public class MoveBlobActivityContext
    {
        public string BlobStorageConnectionString { get; set; }
        public string ContainerName { get; set; }
        public string BlobStorageFolderPath { get; set; }

        public string AzureSubscriptionId { get; set; }
        public string AadDomain { get; set; }
        public string AadClient { get; set; }
        public string AadClientSecret { get; set; }
        public string AdlsName { get; set; }
    }
}