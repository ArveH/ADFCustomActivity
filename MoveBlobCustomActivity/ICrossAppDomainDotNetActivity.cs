using System.Collections.Generic;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace MoveBlobCustomActivityNS
{
    interface ICrossAppDomainDotNetActivity<TExecutionContext>
    {
        IDictionary<string, string> Execute(TExecutionContext context, IActivityLogger logger);
    }
}