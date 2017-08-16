using System.Collections.Generic;
using Microsoft.Azure.Management.DataFactories.Runtime;

// Read this about app-domain isolation for .NET activity:
// https://github.com/Azure/Azure-DataFactory/tree/master/Samples/CrossAppDomainDotNetActivitySample

namespace MoveBlobCustomActivityNS
{
    interface ICrossAppDomainDotNetActivity<TExecutionContext>
    {
        IDictionary<string, string> Execute(TExecutionContext context, IActivityLogger logger);
    }
}