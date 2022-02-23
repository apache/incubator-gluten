package com.intel.oap.jni;

import com.intel.oap.execution.ColumnarNativeIterator;
import com.intel.oap.row.RowIterator;
import com.intel.oap.row.SparkRowInfo;
import com.intel.oap.vectorized.ExpressionEvaluator;
import io.substrait.proto.Plan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestExpressionEvaluator {

    @Before
    public void setup() {
    }

    @Test
    public void testInitNative() throws Exception {
        ExpressionEvaluator transKernel = new ExpressionEvaluator(new ArrayList<String>(),
                "spark_columnar_jni",
                "/home/myubuntu/Works/c_cpp_projects/Kyligence-ClickHouse-MergeTree/cmake-build-debug/utils/local-engine/liblocal_engine_jnid.so",
                false);
        transKernel.initNative();

        FileInputStream in =
                new FileInputStream(this.getClass().getResource("/").getPath() +
                        "/SubStraitTest-Q6.dat");
        DataInputStream dis = new DataInputStream(in);

        // byte[] itemBuf = new byte[1024];
        // dis.read(itemBuf, 0, 8);

        Plan substraitPlan = Plan.parseFrom(dis);

        ArrayList<ColumnarNativeIterator> inBatchIters = new java.util.ArrayList<ColumnarNativeIterator>();
        RowIterator resIter = transKernel.createKernelWithRowIterator(substraitPlan.toByteArray(),
                inBatchIters);
        while (resIter.hasNext()) {
            SparkRowInfo sparkRowInfo = resIter.next();
            System.out.println(sparkRowInfo.offsets.length);
            System.out.println(sparkRowInfo.fieldsNum);
        }
    }
}
