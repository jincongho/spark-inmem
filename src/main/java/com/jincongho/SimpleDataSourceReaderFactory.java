package com.jincongho;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;


public class SimpleDataSourceReaderFactory implements InputPartition<InternalRow> {

    public static class SimpleDataSourcePartition implements InputPartitionReader<InternalRow> {

        private Object[] values;
        private int index = 0;

        public SimpleDataSourcePartition(Object[] data) {
            this.values = data;
        }

        @Override
        public boolean next() throws IOException {
            return index < values.length;
        }

        @Override
        public InternalRow get() {
            return new GenericInternalRow(new Object[]{ values[index++] });
        }

        @Override
        public void close() throws IOException {
        }
    }

    private Object[] data;

    public SimpleDataSourceReaderFactory(Object[] data) {
        this.data = data;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new SimpleDataSourcePartition(data);
    }

}
