package com.microsoft.azure.functions.worker;

import java.util.concurrent.*;


import io.grpc.*;
import io.grpc.stub.*;

import com.microsoft.azure.functions.rpc.messages.*;

/**
 * Grpc client talks with the Azure Functions Runtime Host. It will dispatch to different message handlers according to the inbound message type.
 * Thread-Safety: Single thread.
 */
public class JavaWorkerClient implements AutoCloseable {
    public JavaWorkerClient(String[] args) {
        System.out.println(args[0]);
        System.out.println(args[1]);
        ManagedChannelBuilder<?> chanBuilder = ManagedChannelBuilder.forAddress(args[0], Integer.parseInt(args[1])).usePlaintext(true);
        this.channel = chanBuilder.build();
        this.responseObserver = new ResponseObserverImpl();
    }

    public Future<Void> listen(String workerId, String requestId) {
        this.responseObserver.send(workerId);
        return this.responseObserver.getListeningTask();
    }

    @Override
    public void close() throws Exception {
        this.responseObserver.close();
        this.channel.shutdownNow();
        this.channel.awaitTermination(15, TimeUnit.SECONDS);
    }

    private class ResponseObserverImpl implements StreamObserver<StreamingMessage>, AutoCloseable {
        ResponseObserverImpl() {
            this.task = new CompletableFuture<>();
            //this.threadpool = ForkJoinPool.commonPool();

            this.requestObserver = FunctionRpcGrpc.newStub(JavaWorkerClient.this.channel).eventStream(this);
        }

        @Override
        public synchronized void close() throws Exception {
         //   this.threadpool.shutdown();
         //   this.threadpool.awaitTermination(15, TimeUnit.SECONDS);
            this.requestObserver.onCompleted();
        }


        /**
         * Handles the request. Grpc will not accept the next request until you exit this method.
         * @param message The incoming Grpc generic message.
         */
        @Override
        public void onNext(StreamingMessage message) {
          // this.threadpool.submit(() -> {
            StreamingMessage.Builder messageBuilder = StreamingMessage.newBuilder();
            InvocationResponse.Builder invocationResponse = InvocationResponse.newBuilder();
            invocationResponse.setInvocationId(message.getInvocationRequest().getInvocationId());

            invocationResponse.setResult("Success");
            TypedData.Builder typeData = TypedData.newBuilder();
            RpcHttp.Builder http = RpcHttp.newBuilder();
            TypedData.Builder body = TypedData.newBuilder();
            body.setString("Hello World!!");
            http.setBody(body);
            http.setStatusCode("OK");
            typeData.setHttp(http);
            invocationResponse.setReturnValue(typeData);

            messageBuilder.setInvocationResponse(invocationResponse);
            this.requestObserver.onNext(messageBuilder.build());
          // });
        }

        private synchronized void send(String message) {
            System.out.println("here 1");
            StreamingMessage.Builder messageBuilder = StreamingMessage.newBuilder();
            StartStream.Builder startStream = StartStream.newBuilder();
            startStream.setWorkerId(message);
            messageBuilder.setStartStream(startStream);
            System.out.println("sent:" + messageBuilder.build());
            this.requestObserver.onNext(messageBuilder.build());
        }

        @Override
        public void onCompleted() { this.task.complete(null); }

        @Override
        public void onError(Throwable t) { this.task.completeExceptionally(t); }

        private CompletableFuture<Void> getListeningTask() { return this.task; }


        private CompletableFuture<Void> task;
     //   private ExecutorService threadpool;
        private StreamObserver<StreamingMessage> requestObserver;
    }

    private final ManagedChannel channel;
    private final ResponseObserverImpl responseObserver;
}
