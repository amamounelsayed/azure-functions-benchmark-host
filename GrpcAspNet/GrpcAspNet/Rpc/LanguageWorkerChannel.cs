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
using Grpc.Core;

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
        private RouteGuideClient client;


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

                Channel channel = new Channel("127.0.0.1:49150", ChannelCredentials.Insecure);
                client = new RouteGuideClient(new FunctionRpc.FunctionRpcClient(channel), this);
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to start Language Worker Channel for language", ex);
            }
        }

        internal void SendInvocationRequest(ScriptInvocationContext context)
        {
            _logger.LogInformation($"sending invocation request id: {context.InvocationId}");
            
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

            StreamingMessage msg = new StreamingMessage
            {
                InvocationRequest = invocationRequest
            };


            client.RouteChat(msg);


          //  SendStreamingMessage(new StreamingMessage
          //  {
          //      InvocationRequest = invocationRequest
          //  });
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
            
            _logger.LogInformation($"WriteInvocationRequest id: {context.InvocationId} on threadId: {Thread.CurrentThread.ManagedThreadId}");

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
            _logger.LogInformation($"InvocationResponse received id: {invokeResponse.InvocationId}");

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
            _logger.LogInformation($"RpcWriteEvent Done  id: {writeEvent.InvocationId} on threadId: {Thread.CurrentThread.ManagedThreadId}");

            if (_executingWrites.TryRemove(writeEvent.InvocationId, out RpcWriteContext context))
            {
                context.ResultSource.SetResult($"WriteDone-{writeEvent.InvocationId}");
            }
        }

        private async void SendStreamingMessage(StreamingMessage msg)
        {
            _logger.LogInformation($"SendStreamingMessage...on threadId: {Thread.CurrentThread.ManagedThreadId}");

            _eventManager.Publish(new OutboundEvent(_workerId, msg));
        }

        public void Dispose()
        {
            _process.Dispose();
        }

        public class RouteGuideClient
        {
            readonly FunctionRpc.FunctionRpcClient client;
            LanguageWorkerChannel workerChannel;

            public RouteGuideClient(FunctionRpc.FunctionRpcClient client, LanguageWorkerChannel workerChannel)
            {
                this.client = client;
                this.workerChannel = workerChannel;
            }

            /// <summary>
            /// Blocking unary call example.  Calls GetFeature and prints the response.
            /// </summary>
            /*public void GetFeature(int lat, int lon)
            {
                try
                {
                    Log("*** GetFeature: lat={0} lon={1}", lat, lon);

                    Point request = new Point { Latitude = lat, Longitude = lon };

                    Feature feature = client.GetFeature(request);
                    if (feature.Exists())
                    {
                        Log("Found feature called \"{0}\" at {1}, {2}",
                            feature.Name, feature.Location.GetLatitude(), feature.Location.GetLongitude());
                    }
                    else
                    {
                        Log("Found no feature at {0}, {1}",
                            feature.Location.GetLatitude(), feature.Location.GetLongitude());
                    }
                }
                catch (RpcException e)
                {
                    Log("RPC failed " + e);
                    throw;
                }
            }*/



            /// <summary>
            /// Bi-directional streaming example. Send some chat messages, and print any
            /// chat messages that are sent from the server.
            /// </summary>
            public async Task RouteChat(StreamingMessage msg)
            {
                try
                {
                    using (
                        var call = client.EventStream())
                    {
                        var responseReaderTask = Task.Run(async () =>
                        {
                            while (await call.ResponseStream.MoveNext())
                            { 
                                var currentMessage = call.ResponseStream.Current;

                                if (currentMessage.InvocationResponse != null)
                                {
                                    workerChannel.InvokeResponse(currentMessage.InvocationResponse);
                                }
                                else
                                {
                                    workerChannel._eventManager.Publish(new InboundEvent(workerChannel._workerId, currentMessage));
                                }
                            }
                        });

                        await call.RequestStream.WriteAsync(msg);

                        await call.RequestStream.CompleteAsync();
                        await responseReaderTask;
                    }
                }
                catch (RpcException e)
                {
                    var A = 0L;
                }
            }
        }
    }
}

