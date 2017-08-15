using System;

namespace MoveBlobCustomActivityNS
{
    [Serializable]
    public class MoveBlobActivityContext
    {
        public string BlobStorageConnectionString { get; set; }
        public string ContainerName { get; set; }
        public string BlobStorageFolderPath { get; set; }

        public AdlsInfo AdlsInfo { get; set; }
        public string AdlsFolderPath { get; set; }
    }
}