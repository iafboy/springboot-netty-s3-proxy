package com.example;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;


import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.serialization.StringSerializer;

public class S3ProxyHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final AsyncFileWriterService fileWriterService;

    public S3ProxyHandler(S3AsyncClient s3Client,AsyncFileWriterService fileWriterService_,String bucketName_) {
        this.s3Client = s3Client;
        this.bucketName=bucketName_;
        this.fileWriterService=fileWriterService_;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        System.out.println("--- get Request");
        if (request.method() == HttpMethod.PUT) {
            handlePutRequest(ctx, request);
        }else {
            sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void handlePutRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
        System.out.println("--- PUT Request");
        //Key的值此处举例为key001
        String path=request.uri().toString();
        String key =path.substring(path.lastIndexOf('/') + 1)+"."+ ThreadLocalRandom.current().nextInt(1, 100001);
        //------------------------------------
        //接收PUT请求，并开始处理PUT的内容
        byte[] content = new byte[request.content().readableBytes()];
        request.content().readBytes(content);
        byte[] modifiedContent = modifyContent(content);
        //文件落本地磁盘/共享磁盘
        System.out.println("--- begin to write file "+key);
        String filePath="/home/opc/"+key ;
        fileWriterService.writeFile(filePath, modifiedContent)
                .thenApply(v -> "File written successfully")
                .exceptionally(ex -> "Failed to write file: " + ex.getMessage());
        System.out.println("--- written to file "+filePath);
        //发送消息至Kafka，将文件以及缓存位置发送至消息队列
        /*
        String topic="";
        String message="";
        sendToStreaming(String topic,byte[] message)
         */
        //如果是付费套餐用户的视频文件，则转存入对象存储（建议先进入转存任务队列，然后按照队列顺序优先级等存入对象存储）
        //sendToS3(key,modifiedContent,ctx,request);
        sendOK(ctx);
    }

    private void sendToStreaming(String topic,String message){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cell-1.streaming.ap-singapore-1.oci.oraclecloud.com:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 在 OCI 控制台创建的 Kafka 兼容服务的主题
        String topicName = "stream0bj";

        // 在 OCI 控制台获取的 Kafka 兼容服务密钥
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"XXXXXXX/XXXXXX/ocid1.streampool.oc1.ap-singapore-1.XXXXXXXXXXXXXXX\" password=\"XXXXXXXXXX\";");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");

        // 创建 Kafka 生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try{
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭 Kafka 生产者
            producer.close();
        }
    }
    private void sendToS3(String key,byte[] modifiedContent,ChannelHandlerContext ctx,FullHttpRequest request){
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        CompletableFuture<PutObjectResponse> future = s3Client.putObject(putObjectRequest, AsyncRequestBody.fromBytes(modifiedContent));
        future.whenComplete((resp, err) -> {
            if (err != null) {
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                err.printStackTrace();
            } else {
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            }
        });
    }

    //此处将上发内容进行处理
    private byte[] modifyContent(byte[] content) {
        //----这部分为示例代码，建议根据业务需要对内容进行修改
        byte[] returnData=content;
        //修改视频头内容加密,这样可以做到一用户一密，即使发错给其他用户，其他用户APP也无法打开

        //------------------------------
        return returnData;
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", StandardCharsets.UTF_8));

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
    private void sendOK(ChannelHandlerContext ctx){
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
