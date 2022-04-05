package com.jincongho;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class DefaultSource implements DataSourceV2, ReadSupport, WriteSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
        return new SimpleDataSourceReader();
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String s, StructType structType, SaveMode saveMode, DataSourceOptions dataSourceOptions) {
        return Optional.of(new SimpleDataSourceWriter());
    }
}
