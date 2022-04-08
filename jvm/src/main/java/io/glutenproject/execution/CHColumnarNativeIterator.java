package io.glutenproject.execution;

import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.vectorized.CHColumnVector;
import io.glutenproject.vectorized.CHNativeBlock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class CHColumnarNativeIterator extends ColumnarNativeIterator {

    public CHColumnarNativeIterator(Iterator<ColumnarBatch> delegated) {
        super(delegated);
    }

    @Override
    public byte[] next() {
        ColumnarBatch dep_cb = nextBatch;
        if (dep_cb.numRows() > 0) {
            CHColumnVector col = (CHColumnVector)dep_cb.column(0);
            return longtoBytes(col.getBlockAddress());
        } else {
            throw new IllegalStateException();
        }
    }

    private static byte[] longtoBytes(long data) {
        return new byte[]{
                (byte) ((data >> 56) & 0xff),
                (byte) ((data >> 48) & 0xff),
                (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff),
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff),
        };
    }
}
