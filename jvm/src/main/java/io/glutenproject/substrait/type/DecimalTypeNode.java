package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class DecimalTypeNode implements TypeNode, Serializable {
    private final Boolean nullable;
    private final int precision;
    private final int scale;

    DecimalTypeNode(Boolean nullable, int precision, int scale) {
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public Type toProtobuf() {
        Type.Decimal.Builder decimalBuilder = Type.Decimal.newBuilder();
        decimalBuilder.setPrecision(precision);
        decimalBuilder.setScale(scale);
        if (nullable) {
            decimalBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
        } else {
            decimalBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        }

        Type.Builder builder = Type.newBuilder();
        builder.setDecimal(decimalBuilder.build());
        return builder.build();
    }
}
