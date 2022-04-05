package com.jincongho;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class SimpleDataSourceReader implements DataSourceReader {

    private SimpleData data = new SimpleData();

    @Override
    public StructType readSchema() {
        return new StructType(new StructField[]{
                new StructField("value", StringType, true, Metadata.empty())
        });
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> partitions = new ArrayList<>();
        for(int p=0; p<data.numPartition(); p++)
            partitions.add(new SimpleDataSourceReaderFactory((Object[]) data.getPartition(p)));
        return partitions;
    }

}
