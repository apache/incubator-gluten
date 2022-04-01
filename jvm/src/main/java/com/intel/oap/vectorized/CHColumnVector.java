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

package com.intel.oap.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class CHColumnVector extends ColumnVector {
    private final long blockAddress;
    private final int columnPosition;

    public CHColumnVector(DataType type, long blockAddress, int columnPosition) {
        super(type);
        this.blockAddress = blockAddress;
        this.columnPosition = columnPosition;
    }

    public long getBlockAddress() {
        return blockAddress;
    }

    @Override
    public void close() {

    }

    private native boolean nativeHasNull(long blockAddress, int columnPosition);

    @Override
    public  boolean hasNull() {
        return nativeHasNull(blockAddress, columnPosition);
    }

    private native int nativeNumNulls(long blockAddress, int columnPosition);

    @Override
    public int numNulls(){
        return nativeNumNulls(blockAddress, columnPosition);
    }

    private native boolean nativeIsNullAt(long blockAddress, int columnPosition);

    @Override
    public  boolean isNullAt(int rowId){
        return nativeIsNullAt(blockAddress, columnPosition);
    }

    private native boolean nativeGetBoolean(long blockAddress, int columnPosition);

    @Override
    public boolean getBoolean(int rowId){
        return nativeGetBoolean(blockAddress, columnPosition);
    }
    private native byte nativeGetByte(long blockAddress, int columnPosition);

    @Override
    public byte getByte(int rowId){
        return nativeGetByte(blockAddress, columnPosition);
    }

    private native short nativeGetShort(long blockAddress, int columnPosition);

    @Override
    public short getShort(int rowId){
        return nativeGetShort(blockAddress, columnPosition);
    }

    private native int nativeGetInt(long blockAddress, int columnPosition);

    @Override
    public int getInt(int rowId){
        return nativeGetInt(blockAddress, columnPosition);
    }

    private native long nativeGetLong(long blockAddress, int columnPosition);

    @Override
    public long getLong(int rowId){
        return nativeGetLong(blockAddress, columnPosition);
    }

    private native float nativeGetFloat(long blockAddress, int columnPosition);

    @Override
    public float getFloat(int rowId){
        return nativeGetFloat(blockAddress, columnPosition);
    }

    private native double nativeGetDouble(long blockAddress, int columnPosition);

    @Override
    public double getDouble(int rowId){
        return nativeGetDouble(blockAddress, columnPosition);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        return null;
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return null;
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return null;
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return null;
    }

    @Override
    public byte[] getBinary(int rowId) {
        return new byte[0];
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return null;
    }
}
