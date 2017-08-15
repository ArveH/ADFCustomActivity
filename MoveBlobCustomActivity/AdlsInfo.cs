using System;

namespace MoveBlobCustomActivityNS
{
    [Serializable]
    public class AdlsInfo
    {
        public string AzureSubscriptionId { get; set; }
        public string AadDomain { get; set; }
        public string AadClient { get; set; }
        public string AadClientSecret { get; set; }
        public string AdlsUri { get; set; }
        public string AdlsName { get; set; }
        public string AdlsResourceGroupName { get; set; }
        public string AdlsAuthorization { get; set; }
        public string AdlsSessionId { get; set; }
    }
}