package com.mininglamp.privilege;

/**
 * TODO: Implementation
 */
public class LdwRole {
    // operator is responsible for operating cluster, such as add/drop node
    public static String OPERATOR_ROLE = "operator";
    // admin is like DBA, who has all privileges except for NODE privilege held by operator
    public static String ADMIN_ROLE = "admin";
}
