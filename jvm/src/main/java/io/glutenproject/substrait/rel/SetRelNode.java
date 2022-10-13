package io.glutenproject.substrait.rel;

import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.SetRel;

import java.io.Serializable;
import java.util.ArrayList;

public class SetRelNode implements  RelNode, Serializable {
    private final ArrayList<RelNode> inputs;
    private final SetRel.SetOp opType;

    SetRelNode(ArrayList<RelNode> inputs, SetRel.SetOp opType)
    {
        this.inputs = inputs;
        this.opType = opType;
    }
    @Override
    public Rel toProtobuf() {
        RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
        relCommonBuilder.setDirect(RelCommon.Direct.newBuilder().build());

        SetRel.Builder setBuilder = SetRel.newBuilder();
        this.inputs.forEach( (r) -> setBuilder.addInputs(r.toProtobuf()));
        setBuilder.setOp(this.opType);

        return Rel.newBuilder().setSet(setBuilder.build()).build();
    }
}
