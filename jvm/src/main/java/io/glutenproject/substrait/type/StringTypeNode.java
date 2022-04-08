package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class StringTypeNode implements TypeNode, Serializable {
    private final Boolean nullable;

    StringTypeNode(Boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public Type toProtobuf() {
        Type.String.Builder stringBuilder = Type.String.newBuilder();
        if (nullable) {
            stringBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
        } else {
            stringBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        }

        Type.Builder builder = Type.newBuilder();
        builder.setString(stringBuilder.build());
        return builder.build();
    }
}
