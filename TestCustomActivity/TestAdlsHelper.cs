using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MoveBlobCustomActivityNS;

namespace TestCustomActivity
{
    [TestClass]
    public class TestAdlsHelper
    {
        private static string _adlsName;
        private static AdlsHelper _adlsHelper;

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            var adlsInfo = new AdlsInfo
            {
                AzureSubscriptionId = context.Properties["AzureSubscriptionId"].ToString(),
                AadDomain = context.Properties["AadDomain"].ToString(),
                AadClient = context.Properties["AadClient"].ToString(),
                AadClientSecret = context.Properties["AadClientSecret"].ToString(),
                AdlsName = context.Properties["AdlsName"].ToString()
            };

            _adlsName = adlsInfo.AdlsName;
            _adlsHelper = new AdlsHelper(adlsInfo);
        }

        [TestInitialize]
        public void Setup()
        {
        }

        [TestCleanup]
        public void Cleanup()
        {
        }

        [TestMethod]
        public async Task TestInitializeHelper()
        {
            await Task.FromResult(0);
        }

    }
}