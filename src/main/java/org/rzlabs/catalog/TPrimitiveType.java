package org.rzlabs.catalog;

public enum TPrimitiveType {
    INVALID_TYPE(0),
    NULL_TYPE(1),
    BOOLEAN(2),
    TINYINT(3),
    SMALLINT(4),
    INT(5),
    BIGINT(6),
    FLOAT(7),
    DOUBLE(8),
    DATE(9),
    DATETIME(10),
    BINARY(11),
    DECIMAL_DEPRACTED(12),
    CHAR(13),
    LARGEINT(14),
    VARCHAR(15),
    HLL(16),
    DECIMALV2(17),
    TIME(18),
    OBJECT(19),
    ARRAY(20),
    MAP(21),
    STRUCT(22),
    STRING(23);

    private final int value;

    private TPrimitiveType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TPrimitiveType findByValue(int value) {
        switch (value) {
            case 0:
                return INVALID_TYPE;
            case 1:
                return NULL_TYPE;
            case 2:
                return BOOLEAN;
            case 3:
                return TINYINT;
            case 4:
                return SMALLINT;
            case 5:
                return INT;
            case 6:
                return BIGINT;
            case 7:
                return FLOAT;
            case 8:
                return DOUBLE;
            case 9:
                return DATE;
            case 10:
                return DATETIME;
            case 11:
                return BINARY;
            case 12:
                return DECIMAL_DEPRACTED;
            case 13:
                return CHAR;
            case 14:
                return LARGEINT;
            case 15:
                return VARCHAR;
            case 16:
                return HLL;
            case 17:
                return DECIMALV2;
            case 18:
                return TIME;
            case 19:
                return OBJECT;
            case 20:
                return ARRAY;
            case 21:
                return MAP;
            case 22:
                return STRUCT;
            case 23:
                return STRING;
            default:
                return null;
        }
    }
}
