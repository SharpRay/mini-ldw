package org.rzlabs.privilege;

import org.rzlabs.analysis.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TODO: implementation
 */
public class LdwAuth {
    private static final Logger LOG = LogManager.getLogger(LdwAuth.class);

    // root user's role is operator.
    // each Palo system has only one root user.
    public static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";
    // unknown user does not have any privilege, this is just to be compatible with old version.
    public static final String UNKNOWN_USER = "unknown";

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void readLock() {
        lock.readLock().lock();
    }

    /*
     * check password, if matched, save the userIdentity in matched entry.
     * the following auth checking should use userIdentity saved in currentUser.
     */
    public static boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
                                 List<UserIdentity> currentUser) {
        currentUser.add(UserIdentity.ROOT);
        return true;
    }
}
