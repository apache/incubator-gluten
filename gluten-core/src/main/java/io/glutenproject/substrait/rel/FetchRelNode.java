package io.glutenproject.substrait.rel;

import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.substrait.proto.FetchRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;

public class FetchRelNode implements RelNode, Serializable {
    private final RelNode input;
    private final Long offset;
    private final Long count;

    private final AdvancedExtensionNode extensionNode;

    FetchRelNode(RelNode input, Long offset, Long count) {
        this.input = input;
        this.offset = offset;
        this.count = count;
        this.extensionNode = null;
    }

    FetchRelNode(RelNode input, Long offset, Long count, AdvancedExtensionNode extensionNode) {
        this.input = input;
        this.offset = offset;
        this.count = count;
        this.extensionNode = extensionNode;
    }

    @Override
    public Rel toProtobuf() {
        RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
        relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

        FetchRel.Builder fetchRelBuilder = FetchRel.newBuilder();
        fetchRelBuilder.setCommon(relCommonBuilder.build());
        if (input != null) {
            fetchRelBuilder.setInput(input.toProtobuf());
        }
        fetchRelBuilder.setOffset(offset);
        fetchRelBuilder.setCount(count);

        if (extensionNode != null) {
            fetchRelBuilder.setAdvancedExtension(extensionNode.toProtobuf());
        }

        Rel.Builder relBuilder = Rel.newBuilder();
        relBuilder.setFetch(fetchRelBuilder.build());
        return relBuilder.build();
    }
}
