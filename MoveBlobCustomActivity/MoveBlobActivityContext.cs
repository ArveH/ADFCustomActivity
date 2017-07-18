using System;

namespace MoveBlobCustomActivityNS
{
    [Serializable]
    public class MoveBlobActivityContext
    {
        public string ConnectionString { get; set; }
        public string FolderPath { get; set; }
    }
}