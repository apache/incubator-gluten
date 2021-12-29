package com.intel.oap.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class DateTypeNode implements TypeNode, Serializable {
    private final String name;
    private final Boolean nullable;

    DateTypeNode(String name, Boolean nullable) {
        this.name = name;
        this.nullable = nullable;
    }

    @Override
    public Type toProtobuf() {
        Type.Date.Builder dateBuilder = Type.Date.newBuilder();
        if (nullable) {
            dateBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
        } else {
            dateBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        }
        Type.Builder builder = Type.newBuilder();
        builder.setDate(dateBuilder.build());
        return builder.build();
    }
}
