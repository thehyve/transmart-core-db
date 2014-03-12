package org.transmartproject.db.dataquery.highdim

import groovy.transform.ToString
import org.transmartproject.core.dataquery.assay.Assay
import org.transmartproject.core.dataquery.highdim.AssayColumn

@ToString(includes = ['assay', 'label'])
class AssayColumnImpl implements AssayColumn, Serializable {

    private static final long serialVersionUID = 1L

    @Delegate
    private Assay assay

    final String label

    public AssayColumnImpl(Assay assay) {
        this.assay = assay
        this.label = assay.sampleCode
    }

    @Override
    boolean equals(Object other) {
        other instanceof AssayColumnImpl &&
                this.assay.id == other.assay.id
    }

    @Override
    int hashCode() {
        this.assay.id.hashCode()
    }
}
