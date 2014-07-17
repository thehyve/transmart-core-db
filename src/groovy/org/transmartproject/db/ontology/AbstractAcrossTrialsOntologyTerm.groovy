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

package org.transmartproject.db.ontology

import org.transmartproject.core.dataquery.Patient
import org.transmartproject.core.ontology.OntologyTerm
import org.transmartproject.core.ontology.Study
import org.transmartproject.db.concept.ConceptKey

abstract class AbstractAcrossTrialsOntologyTerm implements OntologyTerm {

    public final static String ACROSS_TRIALS_TABLE_CODE = 'xtrials'
    public final static String ACROSS_TRIALS_TOP_TERM_NAME = "Across Trials"

    /* Relies mainly on fullName; also on level */

    ConceptKey getConceptKey() {
        new ConceptKey(ACROSS_TRIALS_TABLE_CODE, fullName)
    }

    @Override
    String getKey() {
        conceptKey.toString()
    }

    @Override
    String getName() {
        conceptKey.conceptFullName.parts[-1]
    }

    @Override
    String getTooltip() {
        name
    }

    @Override
    Study getStudy() {
        /* null for now. We may want to create a fake study for
         * xtrials purposes */
    }

    @Override
    List<OntologyTerm> getChildren() {
        getDescendants(false)
    }

    @Override
    List<OntologyTerm> getChildren(boolean showHidden, boolean showSynonyms) {
        getDescendants(false, showHidden, showSynonyms)
    }

    @Override
    List<OntologyTerm> getAllDescendants() {
        getDescendants(true)
    }

    @Override
    List<OntologyTerm> getAllDescendants(boolean showHidden, boolean showSynonyms) {
        getAllDescendants(true, showHidden, showSynonyms)
    }

    private List<OntologyTerm> getDescendants(boolean allDescendants,
                                              boolean showHidden = false /* ignored */,
                                              boolean showSynonyms = false /* ignored */) {
        // do not include ACROSS_TRIALS_TOP_TERM_NAME part in prefix:
        String pathPrefix = conceptKey.conceptFullName.length > 1 ?
                "\\${conceptKey.conceptFullName[1..-1].join '\\'}\\" : null

        ModifierDimensionView.withCriteria {
            if (pathPrefix) {
                like 'path', pathPrefix.asLikeLiteral() + '%'
            }
            if (!allDescendants) {
                eq 'level',
                        level - 1L /* "Across Trials" */ + 1L /* children */
            }
        }.collect { new AcrossTrialsOntologyTerm(modifierDimension: it) }
    }

    @Override
    List<Patient> getPatients() {
        throw new UnsupportedOperationException("Retrieving patients for " +
                "x-trial node not supported")
    }
}