package io.glutenproject.execution;

import io.glutenproject.column.ColumnarBatchUtil;
import io.glutenproject.exception.GlutenRuntimeException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SerializableColumnarBatch implements Externalizable, KryoSerializable {

  public SerializableColumnarBatch() {}

  interface ColumnarBatchWriter {
    void write(byte[] data);

    void writeInt(int data);
  }

  interface Reader {
    int read(byte[] data);

    int readInt();
  }

  public static class ObjectOutputWriter implements ColumnarBatchWriter {
    private final ObjectOutput objectOutput;

    public ObjectOutputWriter(ObjectOutput objectOutput) {
      this.objectOutput = objectOutput;
    }

    @Override
    public void write(byte[] data) {
      try {
        objectOutput.write(data);
      } catch (IOException e) {
        throw new GlutenRuntimeException(e);
      }
    }

    @Override
    public void writeInt(int data) {
      try {
        objectOutput.writeInt(data);
      } catch (IOException e) {
        throw new GlutenRuntimeException(e);
      }
    }
  }

  public static class ObjectInputReader implements Reader {
    private final ObjectInput objectInput;

    public ObjectInputReader(ObjectInput objectInput) {
      this.objectInput = objectInput;
    }

    @Override
    public int read(byte[] data) {
      try {
        return objectInput.read(data);
      } catch (IOException e) {
        throw new GlutenRuntimeException(e);
      }
    }

    @Override
    public int readInt() {
      try {
        return objectInput.readInt();
      } catch (IOException e) {
        throw new GlutenRuntimeException(e);
      }
    }
  }

  public static class KryoWriter implements ColumnarBatchWriter {
    private final Output output;

    public KryoWriter(Output output) {
      this.output = output;
    }

    @Override
    public void write(byte[] data) {
      output.write(data);
    }

    @Override
    public void writeInt(int data) {
      output.writeInt(data);
    }
  }

  public static class KryoReader implements Reader {
    private final Input input;

    public KryoReader(Input input) {
      this.input = input;
    }

    @Override
    public int read(byte[] data) {
      return input.read(data);
    }

    @Override
    public int readInt() {
      return input.readInt();
    }
  }

  private transient ColumnarBatch columnarBatch = null;

  public byte[] getBuffer() {
    return buffer;
  }

  private transient byte[] buffer;

  public SerializableColumnarBatch(ColumnarBatch columnarBatch) {
    this.columnarBatch = ColumnarBatchUtil.cloneBatch(columnarBatch);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    internalWrite(new KryoWriter(output));
  }

  @Override
  public void read(Kryo kryo, Input input) {
    internalRead(new KryoReader(input));
  }

  @Override
  public void writeExternal(ObjectOutput out) {
    internalWrite(new ObjectOutputWriter(out));
  }

  @Override
  public void readExternal(ObjectInput in) {
    internalRead(new ObjectInputReader(in));
  }

  private void internalWrite(ColumnarBatchWriter writer) {
    if (buffer != null) {
      throw new IllegalStateException("Cannot re-serialize a batch this way...");
    } else if (columnarBatch == null) {
      throw new IllegalStateException("Cannot re-serialize a batch this way....");
    } else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(4 << 10);
      byte[] buffer1 = new byte[4 << 10];
      ColumnarBatchUtil.writeBatch(columnarBatch, bos, buffer1, buffer1.length);
      byte[] result = bos.toByteArray();
      writer.writeInt(result.length);
      writer.write(result);
      ColumnarBatchUtil.safeDisposeBatch(columnarBatch);
      columnarBatch = null;
    }
  }

  private void internalRead(Reader reader) {
    if (buffer == null) {
      int size = reader.readInt();
      buffer = new byte[size];
      reader.read(buffer);
    }
  }
}
