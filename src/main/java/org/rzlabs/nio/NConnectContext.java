package org.rzlabs.nio;


import org.rzlabs.qe.ConnectContext;
import org.rzlabs.qe.ConnectProcessor;
import org.xnio.StreamConnection;

import java.io.IOException;

/**
 * connect context based on nio.
 */
public class NConnectContext extends ConnectContext {
    protected NMysqlChannel mysqlChannel;

    public NConnectContext(StreamConnection connection) {
        super();
        mysqlChannel = new NMysqlChannel(connection);
    }

    @Override
    public void cleanup() {
        mysqlChannel.close();
        returnRows = 0;
    }

    @Override
    public NMysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        mysqlChannel.startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        mysqlChannel.suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        mysqlChannel.resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        mysqlChannel.stopAcceptQuery();
    }
}
