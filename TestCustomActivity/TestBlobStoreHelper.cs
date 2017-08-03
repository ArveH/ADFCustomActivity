using System;
using System.Collections.Generic;
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
    public class TestBlobStoreHelper
    {
        private static string _blobStoreConnectionString;
        private static CloudBlobClient _cloudBlobClient;
        private static CloudBlobContainer _cloudBlobContainer;
        private static string _containerName;

        private BlobStoreHelper _blobStoreHelper;
        private readonly string _testFileName1 = "testfile1.txt";
        private readonly string _testFileName2 = "testfile2.txt";
        private readonly string _fileContent1 = "some text";
        private readonly string _fileContent2 = "some more text";

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            _blobStoreConnectionString = context.Properties["BlobStoreConnectionString"].ToString();
            _containerName = context.Properties["BlobStoreContainer"].ToString();
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_blobStoreConnectionString);
            _cloudBlobClient = storageAccount.CreateCloudBlobClient();
            _cloudBlobContainer = _cloudBlobClient.GetContainerReference(_containerName);
            _cloudBlobContainer.CreateIfNotExists();
        }

        [TestInitialize]
        public void Setup()
        {
            CreateFile(_testFileName1, _fileContent1);
            CreateFile(_testFileName2, _fileContent2);

            var logMock = new Mock<IActivityLogger>();
            logMock.Setup(l => l.Write(It.IsAny<string>()));

            _blobStoreHelper = new BlobStoreHelper(logMock.Object, _blobStoreConnectionString, _containerName);
        }

        [TestCleanup]
        public void Cleanup()
        {
            DeleteFile(_testFileName1);
            DeleteFile(_testFileName2);
        }

        [TestMethod]
        public void TestListBlobs()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");

            blobs.Count.Should().Be(2);
        }

        [TestMethod]
        public async Task TestGetBlobStream()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(2);

            var txt = await GetBlobTextAsync(blobs[0].Uri);
            txt.Should().Be(_fileContent1);
            txt = await GetBlobTextAsync(blobs[1].Uri);
            txt.Should().Be(_fileContent2);
        }


        [TestMethod]
        public async Task TestDeleteBlobs()
        {
            var blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(2, "because we have blobs");

            await _blobStoreHelper.DeleteBlobsAsync(_cloudBlobContainer.Name, "");
            blobs = _blobStoreHelper.ListBlobs(_containerName, "");
            blobs.Count.Should().Be(0, "because blobs should have been deleted");
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

        private async Task<string> GetBlobTextAsync(Uri uri)
        {
            using (var stream = await _blobStoreHelper.GetBlobStreamAsync(uri))
            {
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }
    }
}
