package com.logicalpractice.voldemortrx;

import com.logicalpractice.voldemortrx.VoldemortProtocolNegotiationHandler.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class VoldemortProtocolNegotiationHandlerTest {

    class CompleteListener implements  VoldemortProtocolNegotiationHandler.ConnectionNegotiationComplete {
        Status status;

        @Override
        public void complete(Channel channel, Status status) {
            this.status = status;
        }

        public Status status() { return status; }
    }
    final CompleteListener completeListener = new CompleteListener();

    @Test
    public void successful() throws Exception {

        EmbeddedChannel channel = new EmbeddedChannel(
                new VoldemortProtocolNegotiationHandler(completeListener, Duration.ZERO)
        );

        ByteBuf protocolRequest = channel.readOutbound();
        String requestedProtocol = protocolRequest.toString(CharsetUtil.UTF_8);

        assertThat(requestedProtocol, equalTo("pb0"));

        // respond with an ok
        ByteBuf protocolResponse = Unpooled.copiedBuffer("ok", CharsetUtil.UTF_8);
        channel.writeInbound(protocolResponse);

        assertThat(completeListener.status(), equalTo(Status.PB0_COMPLETE));
    }

    @Test
    public void successfulWithResponseByteByByte() {

        EmbeddedChannel channel = new EmbeddedChannel(
                new VoldemortProtocolNegotiationHandler(completeListener, Duration.ZERO)
        );

        ByteBuf protocolRequest = channel.readOutbound();
        assertThat(protocolRequest.toString(CharsetUtil.UTF_8), equalTo("pb0"));

        // respond with just the 'o' and then the 'k'
        channel.writeInbound(Unpooled.copiedBuffer("o", CharsetUtil.UTF_8));
        assertThat(completeListener.status(), nullValue());
        channel.writeInbound(Unpooled.copiedBuffer("k", CharsetUtil.UTF_8));
        assertThat(completeListener.status(), equalTo(Status.PB0_COMPLETE));
    }
}
