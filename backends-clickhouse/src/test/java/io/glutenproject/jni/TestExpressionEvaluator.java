package io.glutenproject.jni;

import io.glutenproject.execution.AbstractColumnarNativeIterator;
import io.glutenproject.row.RowIterator;
import io.glutenproject.row.SparkRowInfo;
import io.glutenproject.vectorized.ExpressionEvaluator;
import io.substrait.proto.Plan;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;

public class TestExpressionEvaluator {

    @Before
    public void setup() {
    }

    @Test
    public void testInitNative() throws Exception {
        ExpressionEvaluator transKernel = new ExpressionEvaluator(new ArrayList<String>(),
                "spark_columnar_jni",
                "/usr/local/clickhouse/lib/libch.so",
                null,
                false);
        transKernel.initNative();

        FileInputStream in =
                new FileInputStream(this.getClass().getResource("/").getPath() +
                        "/SubStraitTest-Q6.dat");
        DataInputStream dis = new DataInputStream(in);

        // byte[] itemBuf = new byte[1024];
        // dis.read(itemBuf, 0, 8);

        Plan substraitPlan = Plan.parseFrom(dis);

        ArrayList<AbstractColumnarNativeIterator> inBatchIters = new ArrayList<AbstractColumnarNativeIterator>();
        RowIterator resIter = transKernel.createKernelWithRowIterator(substraitPlan.toByteArray(),
                inBatchIters);
        while (resIter.hasNext()) {
            SparkRowInfo sparkRowInfo = resIter.next();
            System.out.println(sparkRowInfo.offsets.length);
            System.out.println(sparkRowInfo.fieldsNum);
        }
    }
}
