using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Moq;
using MoveBlobCustomActivityNS;

namespace TestCustomActivity
{
    [TestClass]
    public class IntegrationTest
    {
        private static string _containerName;
        private readonly string _testFileName1 = "testfile1.txt";
        private readonly string _testFileName2 = "testfile2.txt";
        private readonly string _fileContent1 = "some text";
        private readonly string _fileContent2 = "some more text";

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            InitializeAdls(context);
            InitializeBlobStorage(context);
        }

        [TestInitialize]
        public void Setup()
        {
            AdlsDeleteFile(_testFileName1);

            BlobStoreCreateFile(_testFileName1, _fileContent1);
            BlobStoreCreateFile(_testFileName2, _fileContent2);
        }

        [TestCleanup]
        public void Cleanup()
        {
            BlobStoreDeleteFile(_testFileName1);
            BlobStoreDeleteFile(_testFileName2);
            AdlsDeleteFile(_testFileName1);
        }

        [TestMethod]
        public void TestBlobStoreHelper_When_ListBlobs()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");

            blobs.Count.Should().Be(2);
        }

        [TestMethod]
        public async Task TestBlobStoreHelper_When_GetBlobStream()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(2);

            var txt = await BlobStoreGetTextAsync(blobs[0].Uri);
            txt.Should().Be(_fileContent1);
            txt = await BlobStoreGetTextAsync(blobs[1].Uri);
            txt.Should().Be(_fileContent2);
        }


        [TestMethod]
        public async Task TestBlobStoreHelper_When_DeleteBlobs()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(2, "because we have blobs");

            await _blobStoreHelper.DeleteBlobsAsync(_cloudBlobContainer.Name, "");
            blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(0, "because blobs should have been deleted");
        }

        [TestMethod]
        public async Task TestAdlsHelper_When_Upload()
        {
            using (var stream = GetTestStream("Sample text"))
            {
                await _adlsHelper.UploadFromStreamAsync(stream, _testFileName1);
            }

            var fileExists = await _adlsHelper.FileExistsAsync(_testFileName1);
            fileExists.Should().BeTrue("because file was uploaded");
        }

        [TestMethod]
        public async Task TestCopyFromBlobStoreToAdls()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(2);

            var adlsPath = $"{_containerName}/{_testFileName1}";
            using (var stream = await _blobStoreHelper.GetBlobStreamAsync(blobs[0].Uri))
            {
                await _adlsHelper.UploadFromStreamAsync(stream, adlsPath);
            }

            var fileExists = await _adlsHelper.FileExistsAsync(adlsPath);
            fileExists.Should().BeTrue();
        }

        #region Adls stuff

        private static AdlsHelper _adlsHelper;

        private static void InitializeAdls(TestContext context)
        {
            var adlsInfo = new AdlsInfo
            {
                AzureSubscriptionId = context.Properties["AzureSubscriptionId"].ToString(),
                AadDomain = context.Properties["AadDomain"].ToString(),
                AadClient = context.Properties["AadClient"].ToString(),
                AadClientSecret = context.Properties["AadClientSecret"].ToString(),
                AdlsName = context.Properties["AdlsName"].ToString()
            };
            _adlsHelper = new AdlsHelper(adlsInfo);
        }

        private void AdlsDeleteFile(string path)
        {
            _adlsHelper.DeleteFile(path).GetAwaiter().GetResult();
        }

        #endregion

        #region Blob storage stuff

        private static CloudBlobContainer _cloudBlobContainer;

        private static BlobStoreHelper _blobStoreHelper;

        private static void InitializeBlobStorage(TestContext context)
        {
            var blobStoreConnectionString = context.Properties["BlobStoreConnectionString"].ToString();
            _containerName = context.Properties["BlobStoreContainer"].ToString();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(blobStoreConnectionString);
            var cloudBlobClient = storageAccount.CreateCloudBlobClient();
            _cloudBlobContainer = cloudBlobClient.GetContainerReference(_containerName);
            _cloudBlobContainer.CreateIfNotExists();

            var logMock = new Mock<IActivityLogger>();
            logMock.Setup(l => l.Write(It.IsAny<string>()));
            _blobStoreHelper = new BlobStoreHelper(logMock.Object, blobStoreConnectionString, _containerName);
        }

        private static void BlobStoreCreateFile(string fileName, string content)
        {
            var blob = _cloudBlobContainer.GetBlockBlobReference(fileName);
            blob.UploadText(content);
        }

        private static void BlobStoreDeleteFile(string fileName)
        {
            var blob = _cloudBlobContainer.GetBlockBlobReference(fileName);
            blob.DeleteIfExists();
        }

        private async Task<string> BlobStoreGetTextAsync(Uri uri)
        {
            using (var stream = await _blobStoreHelper.GetBlobStreamAsync(uri))
            {
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        #endregion

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