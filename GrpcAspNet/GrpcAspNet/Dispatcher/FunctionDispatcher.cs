using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GrpcAspNet
{
    public class FunctionDispatcher : IFunctionDispatcher
    {
        public LanguageWorkerChannel _workerChannel;
        private readonly ILogger _logger;

        public FunctionDispatcher(ILogger<FunctionDispatcher> logger)
        {
            _logger = logger;
        }
        public LanguageWorkerChannel WorkerChannel => _workerChannel;

        public void AddWorkerChannel(LanguageWorkerChannel workerChannel)
        {
      //      _logger.LogInformation($"Adding LanguageWorkerChannel workerId:{workerChannel.Id}");
            _workerChannel = workerChannel;
        }
    }
}
