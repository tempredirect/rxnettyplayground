package com.logicalpractice.voldemortrx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.WrappedByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.ScheduledFuture;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.netty.util.CharsetUtil.UTF_8;

public class VoldemortProtocolNegotiationHandler extends ChannelHandlerAdapter {
    enum Status { PB0_COMPLETE, FAILED, TIMEOUT }

    private final ConnectionNegotiationComplete callback;
    private final Duration timeout;
    private ScheduledFuture<?> timeoutFuture;
    private boolean finished = false;

    @FunctionalInterface
    interface ConnectionNegotiationComplete {
        void complete(Channel channel, Status status);
    }

    public VoldemortProtocolNegotiationHandler(
            ConnectionNegotiationComplete callback,
            Duration timeout
    ) {
        assert callback != null;
        assert timeout != null;
        this.callback = callback;
        this.timeout = timeout;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf msg = Unpooled.copiedBuffer("pb0", UTF_8);
        ctx.channel().writeAndFlush(msg);

        if (timeout.compareTo(Duration.ZERO) > 0) { // < zero
            // todo use the Timer and TimerTask implementations from io.netty.util
            timeoutFuture = ctx.executor().schedule(() -> {
                if (!finished) {
                    finished = true;
                    callback.complete(ctx.channel(), Status.TIMEOUT);
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }
    private ByteBuf initial;

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf bytes = (ByteBuf) msg;
        if (bytes.readableBytes() < 2) {
            if (bytes.readableBytes() > 0)
                initial = bytes;
            return;
        }

        StringBuilder buff = new StringBuilder(2);
        if (initial != null) {
            buff.append(initial.toString(UTF_8));
        }
        buff.append(bytes.readBytes(bytes.readableBytes()).toString(UTF_8));
        ReferenceCountUtil.release(msg);

        String res = buff.toString();

        if (timeoutFuture != null) {
            // prevent the timeout from firing
            timeoutFuture.cancel(false);
        }

        if (finished) {
            return; // maybe should log something at this point because the channel is connected
                    // but there must have been a timeout
        }

        finished = true;
        // inform somebody that we're negotiated
        callback.complete(ctx.channel(), res.equals("ok") ? Status.PB0_COMPLETE : Status.FAILED);
    }
}
