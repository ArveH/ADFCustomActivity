using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;

namespace MoveBlobCustomActivityNS
{
    public class AdlsHelper
    {
        private readonly IActivityLogger _logger;
        private readonly string _adlsName;
        private readonly DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

        public AdlsHelper(IActivityLogger logger, AdlsInfo adlsInfo)
        {
            _logger = logger;
            _adlsName = adlsInfo.AdlsName;
            var creds = GetAccountCredentials(adlsInfo);
            _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);

            var adlsAccountManagementClient = new DataLakeStoreAccountManagementClient(creds)
            {
                SubscriptionId = adlsInfo.AzureSubscriptionId
            };
            adlsAccountManagementClient.Account.List();
        }

        /// <summary>
        /// Copy data from a stream into a file in the Data Lake Store.
        /// </summary>
        /// <param name="fromStream">The data to copy to the Data Lake Store</param>
        /// <param name="toPath">The path in the Data Lake Store. The Data Lake Store name should not be part of this string.</param>
        /// <returns></returns>
        public async Task UploadFromStreamAsync(Stream fromStream, string toPath)
        {
            _logger.Write("Uploading stream to '{0}'...", toPath);
            await _adlsFileSystemClient.FileSystem
                .CreateAsync(_adlsName, toPath, fromStream, true)
                .ConfigureAwait(false);
        }

        public async Task<bool> FileExistsAsync(string path)
        {
            return await _adlsFileSystemClient.FileSystem.PathExistsAsync(_adlsName, path);
        }

        public async Task DeleteFile(string path)
        {
            _logger.Write("Deleting '{0}'...", path);
            await _adlsFileSystemClient.FileSystem.DeleteAsync(_adlsName, path);
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