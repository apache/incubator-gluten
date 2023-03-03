/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.substrait.expression;

import com.google.protobuf.ByteString;
import io.substrait.proto.Expression;
import org.apache.spark.sql.types.Decimal;

import java.io.Serializable;
import java.math.BigDecimal;

public class DecimalLiteralNode implements ExpressionNode, Serializable {
    private final Decimal value;

    public DecimalLiteralNode(Decimal value) {
        ExpressionBuilder.checkDecimalScale(value.scale());
        this.value = value;
    }

    private static final long[] POWER_OF_10 = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
            10_000_000_000_000L,
            100_000_000_000_000L,
            1_000_000_000_000_000L,
            10_000_000_000_000_000L,
            100_000_000_000_000_000L // long max = 9,223,372,036,854,775,807
    };

    private static BigDecimal powerOfTen(int scale) {
        if (scale < POWER_OF_10.length) {
            return new BigDecimal(POWER_OF_10[scale]);
        } else {
            int length = POWER_OF_10.length;
            BigDecimal bd = new BigDecimal(POWER_OF_10[length - 1]);

            for (int i = length - 1; i < scale; i++) {
                bd = bd.multiply(new BigDecimal(10));
            }
            return bd;
        }
    }

    @Override
    public Expression toProtobuf() {
        Expression.Literal.Decimal.Builder decimalBuilder = Expression.Literal.Decimal.newBuilder();
        decimalBuilder.setPrecision(value.precision());
        decimalBuilder.setScale(value.scale());

        byte[] twosComplement =
                encodeDecimalIntoBytes(value.toJavaBigDecimal(), value.scale(), 16);
        decimalBuilder.setValue(ByteString.copyFrom(twosComplement));

        Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();
        literalBuilder.setDecimal(decimalBuilder.build());

        Expression.Builder builder = Expression.newBuilder();
        builder.setLiteral(literalBuilder.build());
        return builder.build();
    }

    private static final byte zero = 0;
    private static final byte minus_one = -1;

    public static byte[] encodeDecimalIntoBytes(BigDecimal decimal, int scale, int byteWidth) {
        BigDecimal scaledDecimal = decimal.multiply(powerOfTen(scale));
        byte[] bytes = scaledDecimal.toBigInteger().toByteArray();
        if (bytes.length > byteWidth) {
            throw new UnsupportedOperationException(
                    "Decimal size greater than " + byteWidth + " bytes: " + bytes.length);
        }
        byte[] encodedBytes = new byte[byteWidth];
        byte padByte = bytes[0] < 0 ? minus_one : zero;
        // Decimal stored as native-endian, need to swap data bytes if LE
        byte[] bytesLE = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            bytesLE[i] = bytes[bytes.length - 1 - i];
        }

        int destIndex = 0;
        for (int i = 0; i < bytes.length; i++) {
            encodedBytes[destIndex++] = bytesLE[i];
        }

        for (int j = bytes.length; j < byteWidth; j++) {
            encodedBytes[destIndex++] = padByte;
        }
        return encodedBytes;
    }

}
