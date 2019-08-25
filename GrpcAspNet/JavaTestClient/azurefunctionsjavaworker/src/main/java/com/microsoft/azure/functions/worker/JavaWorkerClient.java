package com.microsoft.azure.functions.worker;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Level;


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
        this.peer = new AtomicReference<>(null);
    }

    public Future<Void> listen(String workerId, String requestId) {
        this.peer.set(new StreamingMessagePeer());
        this.peer.get().send(workerId);
        return this.peer.get().getListeningTask();
    }

    @Override
    public void close() throws Exception {
        this.peer.get().close();
        this.peer.set(null);
        this.channel.shutdownNow();
        this.channel.awaitTermination(15, TimeUnit.SECONDS);
    }

    private class StreamingMessagePeer implements StreamObserver<StreamingMessage>, AutoCloseable {
        StreamingMessagePeer() {
            this.task = new CompletableFuture<>();
            this.threadpool = ForkJoinPool.commonPool();

            this.observer = FunctionRpcGrpc.newStub(JavaWorkerClient.this.channel).eventStream(this);
        }

        @Override
        public synchronized void close() throws Exception {
            this.threadpool.shutdown();
            this.threadpool.awaitTermination(15, TimeUnit.SECONDS);
            this.observer.onCompleted();
        }

        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        /**
         * Handles the request. Grpc will not accept the next request until you exit this method.
         * @param message The incoming Grpc generic message.
         */
        @Override
        public void onNext(StreamingMessage message) {
           this.threadpool.submit(() -> {
               String invId = message.getInvocationRequest().getInvocationId();

               Date date = new Date();
               System.out.println("opa client start;; " + invId + ";;" + sdf.format(date));

                StreamingMessage.Builder messageBuilder = StreamingMessage.newBuilder();
                InvocationResponse.Builder invocationResponse = InvocationResponse.newBuilder();
                invocationResponse.setInvocationId(invId);

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
                this.observer.onNext(messageBuilder.build());
               Date date2 = new Date();
               System.out.println("opa client end;; " + invId + ";;" + sdf.format(date2));

           });
        }

        private synchronized void send(String message) {
            System.out.println("here 1");
            StreamingMessage.Builder messageBuilder = StreamingMessage.newBuilder();
            StartStream.Builder startStream = StartStream.newBuilder();
            startStream.setWorkerId(message);
            messageBuilder.setStartStream(startStream);
            System.out.println("sent:" + messageBuilder.build());
            this.observer.onNext(messageBuilder.build());
        }

        @Override
        public void onCompleted() { this.task.complete(null); }

        @Override
        public void onError(Throwable t) { this.task.completeExceptionally(t); }

        private CompletableFuture<Void> getListeningTask() { return this.task; }


        private CompletableFuture<Void> task;
        private ExecutorService threadpool;
        private StreamObserver<StreamingMessage> observer;
    }

    private final ManagedChannel channel;
    private final AtomicReference<StreamingMessagePeer> peer;
}
