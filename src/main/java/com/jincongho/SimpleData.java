package com.jincongho;

import org.apache.spark.unsafe.types.UTF8String;

public class SimpleData {

    private Object[] values = new Object[] {
            new UTF8String[]{
                    UTF8String.fromString("1"),
                    UTF8String.fromString("2"),
                    UTF8String.fromString("3"),
                    UTF8String.fromString("4"),
                    UTF8String.fromString("5"),
                    UTF8String.fromString("a")
            },
            new UTF8String[]{
                    UTF8String.fromString("6"),
                    UTF8String.fromString("7"),
                    UTF8String.fromString("8"),
                    UTF8String.fromString("9"),
                    UTF8String.fromString("10")
            }
    };

    public int numPartition() {
        return values.length;
    }

    public Object getPartition(int partition) {
        return values[partition];
    }

}
