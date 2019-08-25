// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using TestGrpc.Messages;
using GrpcMessages.Events;
using MsgType = TestGrpc.Messages.StreamingMessage.ContentOneofCase;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace GrpcAspNet
{
    public class LanguageWorkerChannel : IDisposable
    {
        private readonly TimeSpan processStartTimeout = TimeSpan.FromSeconds(40);
        private readonly TimeSpan workerInitTimeout = TimeSpan.FromSeconds(30);
        private readonly IScriptEventManager _eventManager;
        private readonly ILogger _logger;

        private string _workerId;
        private Process _process;
        private Queue<string> _processStdErrDataQueue = new Queue<string>(3);
        private IObservable<InboundEvent> _inboundWorkerEvents;
        private IObservable<RpcWriteEvent> _writeEvents;
        private ConcurrentDictionary<string, ScriptInvocationContext> _executingInvocations = new ConcurrentDictionary<string, ScriptInvocationContext>();
        private ConcurrentDictionary<string, RpcWriteContext> _executingWrites = new ConcurrentDictionary<string, RpcWriteContext>();
        private List<IDisposable> _inputLinks = new List<IDisposable>();
        private List<IDisposable> _eventSubscriptions = new List<IDisposable>();
        private string _serverUri;

        private static object _functionLoadResponseLock = new object();

        internal LanguageWorkerChannel()
        {
            // To help with unit tests
        }

        internal LanguageWorkerChannel(
           string workerId,
           IScriptEventManager eventManager,
           string serverUri,
           ILogger logger)
        {
            _workerId = workerId;
            _logger = logger;
            _eventManager = eventManager;
            _serverUri = serverUri;
            _inboundWorkerEvents = _eventManager.OfType<InboundEvent>()
                .ObserveOn(new NewThreadScheduler())
                .Where(msg => msg.WorkerId == _workerId);

            _writeEvents = _eventManager.OfType<RpcWriteEvent>()
                .ObserveOn(new NewThreadScheduler())
                .Where(msg => msg.WorkerId == _workerId);

            _eventSubscriptions.Add(_inboundWorkerEvents.Where(msg => msg.MessageType == MsgType.InvocationResponse)
                .ObserveOn(new NewThreadScheduler())
                .Subscribe((msg) => InvokeResponse(msg.Message.InvocationResponse)));

            StartProcess();
        }

        public string Id => _workerId;

        internal Queue<string> ProcessStdErrDataQueue => _processStdErrDataQueue;

        internal Process WorkerProcess => _process;

        internal void StartProcess()
        {
            string clientPath = Environment.GetEnvironmentVariable("GrpcClient");
            string clientCodePath = Environment.GetEnvironmentVariable("GrpcClientCode");
            try
            {
                ProcessStartInfo startInfo;
                if (string.IsNullOrEmpty(clientCodePath))
                {
                    startInfo = new ProcessStartInfo()
                    {
                        FileName = $"{clientPath}",
                        Arguments = $"{ _serverUri.ToString()} {_workerId}"
                    };
                }
                else if (clientPath == "java")
                {
                    startInfo = new ProcessStartInfo()
                    {
                        FileName = $"{clientPath}",
                        Arguments = @"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar " + $"{clientCodePath} { _serverUri.ToString()}  {_workerId}"
                    };
                }
                else
                {
                    startInfo = new ProcessStartInfo()
                    {
                        FileName = $"{clientPath}",
                        Arguments = $"{clientCodePath} --serverUri { _serverUri.ToString()} --workerId {_workerId}"
                    };
                }

                _process = new Process();
                _process.StartInfo = startInfo;
                _process.Start();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to start Language Worker Channel for language", ex);
            }
        }

        internal void SendInvocationRequest(ScriptInvocationContext context)
        {
         //   _logger.LogInformation($"sending invocation request id: {context.InvocationId}");
            
            InvocationRequest invocationRequest = new InvocationRequest()
            {
                FunctionId = context.FunctionId,
                InvocationId = context.InvocationId,
            };

            var headers = new HeaderDictionary();
            headers.Add("content-type", "application/json");
            HttpRequest request = Utilities.CreateHttpRequest("GET", "http://localhost/api/httptrigger-scenarios", headers);
            invocationRequest.InputData.Add(new ParameterBinding()
            {
                Name = "testHttpRequest",
                Data = request.ToRpc()
            });
            _executingInvocations.TryAdd(invocationRequest.InvocationId, context);
            SendStreamingMessage(new StreamingMessage
            {
                InvocationRequest = invocationRequest
            });
        }

        internal RpcHttp GetRpcHttp()
        {
            RpcHttp rpcHttp = new RpcHttp()
            {

            };

            return rpcHttp;
        }

        internal void WriteInvocationRequest(RpcWriteContext context)
        {
            
         //   _logger.LogInformation($"WriteInvocationRequest id: {context.InvocationId} on threadId: {Thread.CurrentThread.ManagedThreadId}");

            _eventSubscriptions.Add(_writeEvents.Where(msg => msg.InvocationId == context.InvocationId)
                   .ObserveOn(NewThreadScheduler.Default)
                   .Subscribe((msg) => RpcWriteEventDone(msg)));

            InvocationRequest invocationRequest = new InvocationRequest()
            {
                InvocationId = context.InvocationId
            };
            _executingWrites.TryAdd(invocationRequest.InvocationId, context);
            var strMsg = new StreamingMessage
            {
                InvocationRequest = invocationRequest
            };

            SendStreamingMessage(strMsg);
        }

        internal void InvokeResponse(InvocationResponse invokeResponse)
        {
         //   _logger.LogInformation($"InvocationResponse received id: {invokeResponse.InvocationId}");

            if (_executingInvocations.TryRemove(invokeResponse.InvocationId, out ScriptInvocationContext context))
            {
                var result = new ScriptInvocationResult()
                {
                    Return = invokeResponse?.ReturnValue?.ToObject()
                };
                context.ResultSource.SetResult(result);
            }
        }

        internal void RpcWriteEventDone(RpcWriteEvent writeEvent)
        {
         //   _logger.LogInformation($"RpcWriteEvent Done  id: {writeEvent.InvocationId} on threadId: {Thread.CurrentThread.ManagedThreadId}");

            if (_executingWrites.TryRemove(writeEvent.InvocationId, out RpcWriteContext context))
            {
                context.ResultSource.SetResult($"WriteDone-{writeEvent.InvocationId}");
            }
        }

        private void SendStreamingMessage(StreamingMessage msg)
        {
         //   _logger.LogInformation($"SendStreamingMessage...on threadId: {Thread.CurrentThread.ManagedThreadId}");

            _eventManager.Publish(new OutboundEvent(_workerId, msg));
        }

        public void Dispose()
        {
            _process.Dispose();
        }
    }
}
