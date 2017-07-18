using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using MoveBlobCustomActivityNS;

namespace TestCustomActivity
{
    [TestClass]
    public class TestBlobStoreHelper
    {
        private static string _blobStoreConnectionString;

        [ClassInitialize]
        public static void TestClassInitialize(TestContext context)
        {
            _blobStoreConnectionString = context.Properties["BlobStoreConnectionString"].ToString();
        }

        [TestMethod]
        public async Task TestGetBlobs()
        {
            var logMock = new Mock<IActivityLogger>();
            logMock.Setup(l => l.Write(It.IsAny<string>()));
;
            var helper = new BlobStoreHelper(logMock.Object, _blobStoreConnectionString);
            var blobs = await helper.GetBlobsAsync("dimpsdata", "");

            blobs.Count.Should().BeGreaterThan(0, "because we have blobs");
        }
    }
}
