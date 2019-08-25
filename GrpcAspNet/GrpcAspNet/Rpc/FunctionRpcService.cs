// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using TestGrpc.Messages;
using GrpcMessages.Events;
using MsgType = TestGrpc.Messages.StreamingMessage.ContentOneofCase;
using Microsoft.Extensions.Logging;

namespace GrpcAspNet
{
    // Implementation for the grpc service
    // TODO: move to WebJobs.Script.Grpc package and provide event stream abstraction
    internal class FunctionRpcService : FunctionRpc.FunctionRpcBase
    {
        private IScriptEventManager _eventManager;
        private ILogger _logger;
        IDictionary<string, IDisposable> outboundEventSubscriptions = new Dictionary<string, IDisposable>();
        private static SemaphoreSlim _syncSemaphore = new SemaphoreSlim(1, 1);

        public FunctionRpcService(IScriptEventManager scriptEventManager, ILogger<FunctionRpcService> logger)
        {
            _eventManager = scriptEventManager;
            _logger = logger;
        }

        public override async Task EventStream(IAsyncStreamReader<StreamingMessage> requestStream, IServerStreamWriter<StreamingMessage> responseStream, ServerCallContext context)
        {
            var cancelSource = new TaskCompletionSource<bool>();
            try
            {
                context.CancellationToken.Register(() => cancelSource.TrySetResult(false));
                IDisposable outboundEventSubscription = null;

                Func<Task<bool>> messageAvailable = async () =>
                {
                    // GRPC does not accept cancellation tokens for individual reads, hence wrapper
                    var requestTask = requestStream.MoveNext(CancellationToken.None);
                    var completed = await Task.WhenAny(cancelSource.Task, requestTask);
                    return completed.Result;
                };

                if (await messageAvailable())
                {
                    string workerId = requestStream.Current.StartStream.WorkerId;
                    _logger.LogInformation($"Received start stream..workerId: {workerId}");
                    if (outboundEventSubscriptions.TryGetValue(workerId, out outboundEventSubscription))
                    {
                        // no-op
                    }
                    else
                    {
                     outboundEventSubscriptions.Add(workerId, _eventManager.OfType<OutboundEvent>()
                            .Where(evt => evt.WorkerId == workerId)
                            .Subscribe(async evt =>
                            {
                                try
                                {
                                    await _syncSemaphore.WaitAsync();
                                    // WriteAsync only allows one pending write at a time
                                    //   _logger.LogInformation($" writeasync invokeId: {evt.Message.InvocationRequest.InvocationId} on threadId: {Thread.CurrentThread.ManagedThreadId}");
                                    DateTime dateValue_1 = DateTime.Now;
                                    _logger.LogInformation($"opa write before..invokeId;; {evt.Message.InvocationRequest.InvocationId}" + ";;" +dateValue_1.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));
                                    await responseStream.WriteAsync(evt.Message);
                                    DateTime dateValue_2 = DateTime.Now;
                                    _logger.LogInformation($"opa write done..invokeId;; {evt.Message.InvocationRequest.InvocationId}" + ";;" + dateValue_2.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));
                                    _eventManager.Publish(new RpcWriteEvent(workerId, evt.Message.InvocationRequest.InvocationId));
                                }
                                finally
                                {
                                    _syncSemaphore.Release();
                                }
                            }));
                    }
                    do
                    {
                        try
                        {
                            DateTime dateValue_3 = DateTime.Now;
                            _logger.LogInformation($"opa request back..invokeId;; {requestStream.Current.InvocationResponse.InvocationId}" + ";;" + dateValue_3.ToString("MM/dd/yyyy hh:mm:ss.fff tt"));
                        }
                        catch (Exception ex)
                        {
                            var a = 5;
                        }
                        _eventManager.Publish(new InboundEvent(workerId, requestStream.Current));
                    }
                    while (await messageAvailable());
                }
            }
            finally
            {
                foreach (var sub in outboundEventSubscriptions)
                {
                    sub.Value?.Dispose();
                }

                // ensure cancellationSource task completes
                cancelSource.TrySetResult(false);
            }
        }
    }
}
