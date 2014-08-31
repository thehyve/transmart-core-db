/*
 * Copyright © 2013-2014 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.rnaseq

import org.transmartproject.core.dataquery.highdim.BioMarkerDataRow
import org.transmartproject.core.dataquery.highdim.Platform
import org.transmartproject.db.dataquery.highdim.AbstractDataRow
import org.transmartproject.core.dataquery.highdim.chromoregion.RegionRow

class RnaSeqDataRow extends AbstractDataRow implements RegionRow, BioMarkerDataRow<Object> {

    Long    regionId
    String  name
    String  cytoband
    String  chromosome
    Long    start
    Long    end
    Integer numberOfProbes
    String  geneSymbol
    Long    geneId

    Long getId() { regionId }

    Platform getPlatform() {
        throw new UnsupportedOperationException('Getter for get platform is not implemented')
    }

    @Override
    String getLabel() {
        name
    }

    @Override
    String getBioMarker() {
        geneSymbol
    }

    @Override
    public java.lang.String toString() {
        return "RnaSeqDataRow{" +
                "regionId=" + regionId +
                ", data=" + data.toListString() +
                ", assayIndexMap=" + assayIndexMap.toMapString() +
                '}';
    }

}
