package org.transmartproject.db.dataquery.highdim

import org.transmartproject.core.dataquery.DataRow
import org.transmartproject.core.dataquery.highdim.AssayColumn

abstract class AbstractDataRow implements DataRow<AssayColumn, Object>, Serializable {

    private static final long serialVersionUID = 1L

    Map<AssayColumn, Integer> assayIndexMap

    List<Object> data

    @Override
    Object getAt(int index) {
        data[index]
    }

    @Override
    Object getAt(AssayColumn column) {
        data[assayIndexMap[column]]
    }

    @Override
    Iterator<Object> iterator() {
        data.iterator()
    }

}
