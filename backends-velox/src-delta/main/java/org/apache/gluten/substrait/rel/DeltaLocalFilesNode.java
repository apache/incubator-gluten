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
package org.apache.gluten.substrait.rel;

import com.google.protobuf.ByteString;
import io.substrait.proto.ReadRel;
import org.apache.gluten.exception.GlutenException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor;
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray;
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat$;
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DeltaLocalFilesNode extends LocalFilesNode {
  private final List<DeletionVectorInfo> dvInfos;

  public DeltaLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Long> fileSizes,
      List<Long> modificationTimes,
      List<Map<String, String>> partitionColumns,
      List<Map<String, String>> metadataColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      Map<String, String> properties,
      List<Map<String, Object>> otherMetadataColumns,
      List<DeletionVectorInfo> dvInfos) {
    super(
        index,
        paths,
        starts,
        lengths,
        fileSizes,
        modificationTimes,
        partitionColumns,
        metadataColumns,
        fileFormat,
        preferredLocations,
        properties,
        otherMetadataColumns);
    this.dvInfos = dvInfos;
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    final ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.Builder deltaOptionsBuilder =
        ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions.newBuilder();
    final NativeDvDescriptor nativeDvDescriptor = dvInfos.get(index).toNativeDvDescriptor();
    deltaOptionsBuilder.setDvIfContained(nativeDvDescriptor.ifContainedFlag);
    deltaOptionsBuilder.setDvSerializedBitmap(ByteString.copyFrom(nativeDvDescriptor.serializedBitmap));
    final ReadRel.LocalFiles.FileOrFiles.DeltaReadOptions deltaOptions =
        deltaOptionsBuilder.build();
    fileBuilder.setDelta(deltaOptions);
  }

  public interface DeletionVectorInfo extends Serializable {
    NativeDvDescriptor toNativeDvDescriptor();
  }

  public static class KeepAllRowsDeletionVectorInfo implements DeletionVectorInfo {
    @Override
    public NativeDvDescriptor toNativeDvDescriptor() {
      return new NativeDvDescriptor(true,
          new RoaringBitmapArray().serializeAsByteArray(RoaringBitmapArrayFormat$.MODULE$.Portable()));
    }
  }

  public static class DropAllRowsDeletionVectorInfo implements DeletionVectorInfo {
    @Override
    public NativeDvDescriptor toNativeDvDescriptor() {
      return new NativeDvDescriptor(false,
          new RoaringBitmapArray().serializeAsByteArray(RoaringBitmapArrayFormat$.MODULE$.Portable()));
    }
  }

  public static class RegularDeletionVectorInfo implements DeletionVectorInfo {
    private final DeletionVectorDescriptor dvDescriptor;
    private final RowIndexFilterType rowIndexFilterType;
    private final SerializableConfiguration hadoopConf;
    private final String tablePath;

    public RegularDeletionVectorInfo(DeletionVectorDescriptor dvDescriptor, RowIndexFilterType rowIndexFilterType, SerializableConfiguration hadoopConf, String tablePath) {
      this.dvDescriptor = dvDescriptor;
      this.rowIndexFilterType = rowIndexFilterType;
      this.hadoopConf = hadoopConf;
      this.tablePath = tablePath;
    }

    @Override
    public NativeDvDescriptor toNativeDvDescriptor() {
      final boolean ifContainedFlag;
      switch (rowIndexFilterType) {
        case IF_CONTAINED:
          ifContainedFlag = true;
          break;
        case IF_NOT_CONTAINED:
          ifContainedFlag = false;
          break;
        default:
          throw new GlutenException("Unexpected row-index filter type: " + rowIndexFilterType);
      }
      final byte[] bitmapData;
      if (dvDescriptor.isInline()) {
        bitmapData = dvDescriptor.inlineData();
      } else if (dvDescriptor.isOnDisk()) {
        final Path onDiskPath = dvDescriptor.absolutePath(new Path(tablePath));
        try (final FileSystem fs = onDiskPath.getFileSystem(hadoopConf.value());
             final FSDataInputStream reader = fs.open(onDiskPath)) {
          reader.seek(dvDescriptor.offset().getOrElse(() -> 0));
          bitmapData = DeletionVectorStore.readRangeFromStream(reader, dvDescriptor.sizeInBytes());
        } catch (IOException e) {
          throw new GlutenException(e);
        }
      } else {
        throw new GlutenException("Non-empty deletion vector should be either inlined or on disk");
      }
      return new NativeDvDescriptor(ifContainedFlag, bitmapData);
    }
  }

  public static class NativeDvDescriptor implements Serializable {
    public final boolean ifContainedFlag;
    public final byte[] serializedBitmap;

    private NativeDvDescriptor(boolean ifContainedFlag, byte[] serializedBitmap) {
      this.ifContainedFlag = ifContainedFlag;
      this.serializedBitmap = serializedBitmap;
    }
  }
}
