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
        public string AdlsAuthorization { get; set; } // Not used since we're using service-to-service authentication with client secret
        public string AdlsSessionId { get; set; } // Not used since we're using service-to-service authentication with client secret
    }
}