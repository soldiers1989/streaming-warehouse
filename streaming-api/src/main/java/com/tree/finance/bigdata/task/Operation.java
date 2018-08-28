package com.tree.finance.bigdata.task;

/**
 * @author Zhengsj
 * Description: 对应debeziu-core中的：io.debezium.data.Envelope中的Operation枚举
 * Created in 2018/7/12 13:07
 */
public enum Operation {

    /**
     * The operation that read the current state of a record, most typically during snapshots.
     */
    READ("r"),
    /**
     * An operation that resulted in a new record being created in the source.
     */
    CREATE("c"),
    /**
     * An operation that resulted in an existing record being updated in the source.
     */
    UPDATE("u"),
    /**
     * An operation that resulted in an existing record being removed from or deleted in the source.
     */
    DELETE("d");
    private final String code;

    private Operation(String code) {
        this.code = code;
    }

    public static Operation forCode(String code) {
        for (Operation op : Operation.values()) {
            if (op.code().equalsIgnoreCase(code)) return op;
        }
        return null;
    }

    public String code() {
        return code;
    }
}
