package org.rzlabs.nio;


import org.rzlabs.qe.ConnectContext;
import org.rzlabs.qe.ConnectProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.XnioIoThread;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * listener for handle mysql cmd.
 */
public class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {
    private final Logger LOG = LogManager.getLogger(this.getClass());
    private NConnectContext ctx;
    private ConnectProcessor connectProcessor;

    public ReadListener(NConnectContext nConnectContext, ConnectProcessor connectProcessor) {
        this.ctx = nConnectContext;
        this.connectProcessor = connectProcessor;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        // suspend must be call sync in current thread (the IO-Thread notify the read event),
        // otherwise multi handler(task thread) would be waked up by once query.
        XnioIoThread.requireCurrentThread();
        ctx.suspendAcceptQuery();
        // start async query handle in task thread.
        channel.getWorker().execute(() -> {
            ctx.setThreadLocalInfo();
            try {
                connectProcessor.processOnce();
                if (!ctx.isKilled()) {
                    ctx.resumeAcceptQuery();
                } else {
                    ctx.stopAcceptQuery();
                    ctx.cleanup();
                }
            } catch (Exception e) {
                LOG.warn("Exception happened in one session(" + ctx + ").", e);
                ctx.setKilled();
                ctx.cleanup();
            } finally {
                ConnectContext.remove();
            }
        });
    }
}
