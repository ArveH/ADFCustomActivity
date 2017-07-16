using System.Collections.Generic;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace MoveBlobCustomActivityNS
{
    public class MoveBlobCustomActivity: IDotNetActivity
    {
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            logger.Write("\nAll Linked Services(s) Below ");
            foreach (LinkedService ls in linkedServices)
                logger.Write("\nLinked Service: " + ls.Name);

            logger.Write("\nAll Dataset(s) Below ");
            foreach (Dataset ds in datasets)
                logger.Write("\n\tDataset: " + ds.Name);

            return new Dictionary<string, string>();
        }
    }
}
