package io.glutenproject.vectorized;

import io.glutenproject.GazelleJniConfig;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.Iterator;

import java.io.IOException;


public class ColumnarFactory {
    private ColumnarFactory() {
    }

    public static boolean isEnableCH() {
        return GazelleJniConfig.getConf().loadch();
    }

    public static Iterator<ColumnarBatch> createClosableIterator(Iterator<ColumnarBatch> iter) {
        if (isEnableCH()) {
            return new CloseableCHColumnBatchIterator(iter);
        } else {
            return new CloseableColumnBatchIterator(iter);
        }
    }

    public static Object createShuffleSplitterJniWrapper() throws IOException {
        if (isEnableCH()) {
            return new CHShuffleSplitterJniWrapper();
        } else {
            return new ShuffleSplitterJniWrapper();
        }
    }

    public static Serializer createColumnarBatchSerializer(StructType schema, SQLMetric readBatchNumRows, SQLMetric numOutputRows) {
        if (isEnableCH()) {
            return new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows);
        } else {
            return new ArrowColumnarBatchSerializer(schema, readBatchNumRows, numOutputRows);
        }
    }
}
