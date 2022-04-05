package com.jincongho;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;

public class SimpleDataSourceWriterFactory implements DataWriterFactory<InternalRow> {

    public static class SimpleDataSourcePartitionWriter implements DataWriter<InternalRow> {

        @Override
        public void write(InternalRow internalRow) throws IOException {
            System.out.println("Write: " + internalRow.getString(0));
        }

        @Override
        public WriterCommitMessage commit() throws IOException {
            return null;
        }

        @Override
        public void abort() throws IOException {

        }

    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int i, long l, long l1) {
        return new SimpleDataSourcePartitionWriter();
    }
}
