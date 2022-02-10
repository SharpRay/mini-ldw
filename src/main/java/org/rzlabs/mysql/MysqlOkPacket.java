package org.rzlabs.mysql;

import com.google.common.base.Strings;
import org.rzlabs.qe.QueryState;

public class MysqlOkPacket extends MysqlPacket {
    private static final int PACKET_OK_INDICATOR = 0X00;
    // TODO(zhaochun): following are not used in palo
    private static final long LAST_INSERT_ID = 0;
    private final String infoMessage;
    private long affectedRows = 0;
    private int warningRows = 0;
    private int serverStatus = 0;

    public MysqlOkPacket(QueryState state) {
        infoMessage = state.getInfoMessage();
        affectedRows = state.getAffectedRows();
        warningRows = state.getWarningRows();
        serverStatus = state.serverStatus;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // used to check
        MysqlCapability capability = serializer.getCapability();

        serializer.writeInt1(PACKET_OK_INDICATOR);
        serializer.writeVInt(affectedRows);
        serializer.writeVInt(LAST_INSERT_ID);
        if (capability.isProtocol41()) {
            serializer.writeInt2(serverStatus);
            serializer.writeInt2(warningRows);
        } else if (capability.isTransactions()) {
            serializer.writeInt2(serverStatus);
        }

        if (capability.isSessionTrack()) {
            serializer.writeLenEncodedString(infoMessage);
            // TODO(zhaochun): STATUS_FLAGS
            // if ((STATUS_FLAGS & MysqlStatusFlag.SERVER_SESSION_STATE_CHANGED) != 0) {
            // }
        } else {
            if (!Strings.isNullOrEmpty(infoMessage)) {
                // NOTE: in datasheet, use EOF string, but in the code, mysql use length encoded string
                serializer.writeLenEncodedString(infoMessage);
            }
        }
    }
}
