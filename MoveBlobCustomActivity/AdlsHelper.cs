using System.Threading;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;

namespace MoveBlobCustomActivityNS
{
    public class AdlsHelper
    {
        private string _adlsName;
        private readonly DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

        public AdlsHelper(AdlsInfo adlsInfo)
        {
            var creds = GetAccountCredentials(adlsInfo);
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);

            var adlsAccountManagementClient = new DataLakeStoreAccountManagementClient(creds)
            {
                SubscriptionId = adlsInfo.AzureSubscriptionId
            };
            adlsAccountManagementClient.Account.List();
        }

        private ServiceClientCredentials GetAccountCredentials(AdlsInfo adlsInfo)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var domain = adlsInfo.AadDomain;
            var webAppClientId = adlsInfo.AadClient;
            var clientSecret = adlsInfo.AadClientSecret;
            var clientCredential = new ClientCredential(webAppClientId, clientSecret);
            return ApplicationTokenProvider.LoginSilentAsync(domain, clientCredential).Result;
        }
    }
}