﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace MoveBlobCustomActivityNS
{
    public abstract class CrossAppDomainDotNetActivity<TExecutionContext>
        : MarshalByRefObject, IActivityLogger, ICrossAppDomainDotNetActivity<TExecutionContext>, IDotNetActivity
        where TExecutionContext : class
    {
        IActivityLogger _logger;

        IDictionary<string, string> IDotNetActivity.Execute(
            IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, 
            Activity activity, 
            IActivityLogger logger)
        {
            TExecutionContext context = PreExecute(
                linkedServices, datasets, activity, logger);

            Type myType = GetType();
            var assemblyLocation = new FileInfo(myType.Assembly.Location);
            var appDomainSetup = new AppDomainSetup
            {
                ApplicationBase = assemblyLocation.DirectoryName,
                ConfigurationFile = assemblyLocation.Name + ".config"
            };
            AppDomain appDomain = AppDomain.CreateDomain(myType.ToString(), null, appDomainSetup);
            var proxy = (ICrossAppDomainDotNetActivity<TExecutionContext>)
                appDomain.CreateInstanceAndUnwrap(myType.Assembly.FullName, myType.FullName);
            _logger = logger;
            return proxy.Execute(context, (IActivityLogger)this);
        }

        public abstract IDictionary<string, string> Execute(TExecutionContext context, IActivityLogger logger);

        public override object InitializeLifetimeService()
        {
            // Ensure that the client-activated object lives as long as the hosting app domain.
            return null;
        }

        protected virtual TExecutionContext PreExecute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            return null;
        }

        void IActivityLogger.Write(string format, params object[] args)
        {
            _logger.Write(format, args);
        }
    }
}