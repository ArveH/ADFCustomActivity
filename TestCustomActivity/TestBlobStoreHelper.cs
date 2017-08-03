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
    public class TestBlobStoreHelper
    {
        private static string _blobStoreConnectionString;
        private static CloudBlobClient _cloudBlobClient;
        private static CloudBlobContainer _cloudBlobContainer;

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            _blobStoreConnectionString = context.Properties["BlobStoreConnectionString"].ToString();
            var blobContainerName = context.Properties["BlobStoreContainer"].ToString();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_blobStoreConnectionString);
            _cloudBlobClient = storageAccount.CreateCloudBlobClient();
            _cloudBlobContainer = _cloudBlobClient.GetContainerReference(blobContainerName);
            _cloudBlobContainer.CreateIfNotExists();
        }

        [TestInitialize]
        public void Setup()
        {
            CreateFile("testfile1.txt", "Some text");
            CreateFile("testfile2.txt", "Some text here too");
        }

        [TestCleanup]
        public void Cleanup()
        {
            DeleteFile("testfile1.txt");
            DeleteFile("testfile2.txt");
        }

        [TestMethod]
        public async Task TestGetBlobs()
        {
            var logMock = new Mock<IActivityLogger>();
            logMock.Setup(l => l.Write(It.IsAny<string>()));
;
            var helper = new BlobStoreHelper(logMock.Object, _blobStoreConnectionString);
            var blobs = await helper.GetBlobsAsync("dimpsdata", "");

            blobs.Count.Should().BeGreaterThan(2, "because we have blobs");
        }

        [TestMethod]
        public async Task TestDeleteBlobs()
        {
            var logMock = new Mock<IActivityLogger>();
            logMock.Setup(l => l.Write(It.IsAny<string>()));

            var helper = new BlobStoreHelper(logMock.Object, _blobStoreConnectionString);
            var blobs = await helper.GetBlobsAsync("dimpsdata", "");
            blobs.Count.Should().BeGreaterThan(2, "because we have blobs");

            await helper.DeleteBlobsAsync(_cloudBlobContainer.Name, "");
            blobs = await helper.GetBlobsAsync("dimpsdata", "");
            blobs.Count.Should().BeGreaterThan(0, "because blobs should have been deleted");
        }


        private static void CreateFile(string fileName, string content)
        {
            var blob = _cloudBlobContainer.GetBlockBlobReference(fileName);
            blob.UploadText(content);
        }

        private static void DeleteFile(string fileName)
        {
            var blob = _cloudBlobContainer.GetBlockBlobReference(fileName);
            blob.DeleteIfExists();
        }
    }
}
