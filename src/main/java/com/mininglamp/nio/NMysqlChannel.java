package com.mininglamp.nio;


import com.mininglamp.mysql.MysqlChannel;
import com.mininglamp.qe.ConnectProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * mysql Channel based on nio.
 */
public class NMysqlChannel extends MysqlChannel {
    protected final Logger LOG = LogManager.getLogger(this.getClass());
    private StreamConnection conn;

    public NMysqlChannel(StreamConnection connection) {
        super();
        this.conn = connection;
        if (connection.getPeerAddress() instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) connection.getPeerAddress();
            remoteHostPortString = address.getHostString() + ":" + address.getPort();
            remoteIp = address.getAddress().getHostAddress();
        } else {
            // Reach here, what's it?
            remoteHostPortString = connection.getPeerAddress().toString();
            remoteIp = connection.getPeerAddress().toString();
        }
    }

    /**
     * read packet until whole dstBuf is filled, unless block.
     * Todo: find a better way to avoid block read here.
     *
     * @param dstBuf
     * @return
     */
    @Override
    protected int readAll(ByteBuffer dstBuf) {
        int readLen = 0;
        try {
            while (dstBuf.remaining() != 0) {
                int ret = Channels.readBlocking(conn.getSourceChannel(), dstBuf);
                // return -1 when remote peer close the channel
                if (ret == -1) {
                    return readLen;
                }
                readLen += ret;
            }
        } catch (IOException e) {
            LOG.debug("Read channel exception, ignore.", e);
            return 0;
        }
        return readLen;
    }

    /**
     * write packet until no data is remained, unless block.
     *
     * @param buffer
     * @throws IOException
     */
    @Override
    protected void realNetSend(ByteBuffer buffer) throws IOException {
        long bufLen = buffer.remaining();
        long writeLen = Channels.writeBlocking(conn.getSinkChannel(), buffer);
        if (bufLen != writeLen) {
            throw new IOException("Write mysql packet failed.[write=" + writeLen
                    + ", needToWrite=" + bufLen + "]");
        }
        Channels.flushBlocking(conn.getSinkChannel());
        isSend = true;
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (IOException e) {
            LOG.warn("Close channel exception, ignore.");
        }
    }

    public void startAcceptQuery(NConnectContext nConnectContext, ConnectProcessor connectProcessor) {
        conn.getSourceChannel().setReadListener(new ReadListener(nConnectContext, connectProcessor));
        conn.getSourceChannel().resumeReads();
    }

    public void suspendAcceptQuery() {
        conn.getSourceChannel().suspendReads();
    }

    public void resumeAcceptQuery() {
        conn.getSourceChannel().resumeReads();
    }

    public void stopAcceptQuery() throws IOException {
        conn.getSourceChannel().shutdownReads();
    }
}
