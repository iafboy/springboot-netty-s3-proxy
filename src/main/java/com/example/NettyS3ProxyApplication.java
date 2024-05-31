package com.example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import javax.net.ssl.SSLEngine;
import java.net.URI;

@SpringBootApplication
public class NettyS3ProxyApplication implements CommandLineRunner {

    @Value("${nettyServer.port}")
    private int port;

    @Value("${aws.endpointURL}")
    private String endPointURL;

    @Value("${aws.accessKeyId}")
    private String accessKeyId;

    @Value("${aws.secretAccessKey}")
    private String secretAccessKey;

    @Value("${aws.regionId}")
    private String regionId;

    @Value("${aws.bucketName}")
    private String bucketName;

    @Value("${server.workThreadCount}")
    private int workThreadCount;

    @Value("${server.ssl.passChar}")
    private String passChar;

    @Value("${server.ssl.certPath}")
    private String certPath;

    @Autowired
    private AsyncFileWriterService fileWriterService;

    public static void main(String[] args) {
        SpringApplication.run(NettyS3ProxyApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        EventLoopGroup bossGroup = Epoll.isAvailable()? new EpollEventLoopGroup(1):new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(workThreadCount);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(Epoll.isAvailable()? EpollServerSocketChannel.class:NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            //证书相关处理
                            /*
                            SSLEngine sslEngine=SSLFactory.getSslContext(passChar.toCharArray(),certPath).createSSLEngine();
                            sslEngine.setUseClientMode(false);
                            ch.pipeline().addLast(new SslHandler(sslEngine));
                            */
                            //其他
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(65536));
                            ch.pipeline().addLast(new HttpServerExpectContinueHandler());
                            ch.pipeline().addLast(new ChunkedWriteHandler());
                            ch.pipeline().addLast(new S3ProxyHandler(createS3Client(),fileWriterService,bucketName));
                        }
                    })
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("Netty Proxy Server start at "+port);
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private S3AsyncClient createS3Client() {
        return S3AsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .endpointOverride(URI.create(endPointURL)).region(Region.of(regionId))
                .build();
    }
}
