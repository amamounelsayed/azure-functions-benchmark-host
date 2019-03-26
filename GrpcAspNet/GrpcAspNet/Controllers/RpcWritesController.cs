﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace GrpcAspNet.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RpcWritesController : ControllerBase
    {
        private LanguageWorkerChannel _languageWorkerChannel;
        private IFunctionDispatcher _functionDispatcher;
        private ILogger _logger;

        public RpcWritesController(IFunctionDispatcher functionDispatcher, ILogger<RpcWritesController> logger)
        {
            _functionDispatcher = functionDispatcher;
            _logger = logger;
        }
        // GET: api/RcpWrites
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET: api/RcpWrites/5
        [HttpGet("{id}")]
        public Task<string> Get(int id)
        {
            _logger.LogInformation($"APi call received on threadId {Thread.CurrentThread.ManagedThreadId}");
            if (_languageWorkerChannel == null)
            {
                _languageWorkerChannel = _functionDispatcher.WorkerChannel;
            }
            RpcWriteContext writeContext = new RpcWriteContext()
            {
                InvocationId = Guid.NewGuid().ToString(),
                ResultSource = new TaskCompletionSource<string>()
            };
            Task.Factory.StartNew(() => _languageWorkerChannel.WriteInvocationRequestAsync(writeContext));
            return writeContext.ResultSource.Task;
        }

        // POST: api/RcpWrites
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT: api/RcpWrites/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
