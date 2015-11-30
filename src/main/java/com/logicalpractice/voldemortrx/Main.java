package com.logicalpractice.voldemortrx;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.oracle.tools.packager.Log;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;

import java.net.InetAddress;
import java.net.SocketAddress;

/**
 *
 */
public class Main {

    public static void main(String[] args) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        try {
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addFirst(new LoggingHandler(LogLevel.INFO));
                    ch.pipeline().addLast(new VoldemortHandler());

                }
            });

            ChannelFuture f = b.connect(InetAddress.getLocalHost(), 6666).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    static class ExceptionReporter extends ChannelHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class VoldemortHandler extends ExceptionReporter {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            super.connect(ctx, remoteAddress, localAddress, promise);
            System.err.println("connect(" + localAddress + " -> " + remoteAddress + ")");
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ByteBuf msg = Unpooled.copiedBuffer("pb0", CharsetUtil.UTF_8);
            ctx.channel().writeAndFlush(msg)
                    .addListener(future -> { System.err.println("written protocol request"); });
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            System.err.println("channelRead(" + msg + ")");
            ByteBuf bytes = (ByteBuf) msg;
            System.err.println(bytes.readableBytes());
            byte [] byteMsg = new byte[2];
            bytes.readBytes(byteMsg);
            String res = new String(byteMsg, "utf-8");
            System.err.println(res);

            ChannelPipeline pipeline = ctx.channel().pipeline();
            pipeline.remove(this);

            pipeline.addLast("frameLength", new LengthFieldPrepender(4));
            pipeline.addLast("protobufEncoder", new ProtobufEncoder());

            pipeline.addLast("frameLengthDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
            pipeline.addLast("protobufDecoder", new ProtobufDecoder(VProto.GetResponse.getDefaultInstance()));

            pipeline.addLast("handler", new VoldemortClientHandler());
            pipeline.addLast("exception", new ExceptionReporter());


            VProto.VoldemortRequest request = VProto.VoldemortRequest.newBuilder()
                    .setType(VProto.RequestType.GET)
                    .setShouldRoute(false)
                    .setStore("metadata")
                    .setGet(VProto.GetRequest.newBuilder().setKey(ByteString.copyFromUtf8("cluster.xml")))
                    .build();

            ctx.channel().writeAndFlush(request).addListener(future -> { System.err.println("finished writing"); future.get(); });
        }

        static class VoldemortClientHandler extends SimpleChannelInboundHandler<VProto.GetResponse> {

            @Override
            protected void messageReceived(ChannelHandlerContext ctx, VProto.GetResponse msg) throws Exception {
                System.err.println("received:" + msg);
            }
        }
    }


}
