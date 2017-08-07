using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MoveBlobCustomActivityNS;

namespace TestCustomActivity
{
    [TestClass]
    public class TestAdlsHelper
    {
        private const string TestFileName = "integrationtest\\sample.txt";
        private static AdlsInfo _adlsInfo;
        private static AdlsHelper _adlsHelper;

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            _adlsInfo = new AdlsInfo
            {
                AzureSubscriptionId = context.Properties["AzureSubscriptionId"].ToString(),
                AadDomain = context.Properties["AadDomain"].ToString(),
                AadClient = context.Properties["AadClient"].ToString(),
                AadClientSecret = context.Properties["AadClientSecret"].ToString(),
                AdlsName = context.Properties["AdlsName"].ToString()
            };

            _adlsHelper = new AdlsHelper(_adlsInfo);
        }

        [TestInitialize]
        public void Setup()
        {
            _adlsHelper.DeleteFile(TestFileName).GetAwaiter().GetResult();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _adlsHelper.DeleteFile(TestFileName).GetAwaiter().GetResult();
        }

        [TestMethod]
        public async Task TestUpload()
        {
            using (var stream = GetTestStream("Sample text"))
            {
                await _adlsHelper.UploadFromStreamAsync(stream, TestFileName);
            }

            var fileExists = await _adlsHelper.FileExistsAsync(TestFileName);
            fileExists.Should().BeTrue("because file was uploaded");
        }

        private Stream GetTestStream(string txt)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(txt);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}