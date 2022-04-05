package com.jincongho;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class SimpleDataSourceWriter implements DataSourceWriter {
    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SimpleDataSourceWriterFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {

    }
}
