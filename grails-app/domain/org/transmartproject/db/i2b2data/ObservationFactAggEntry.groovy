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

package org.transmartproject.db.i2b2data

class ObservationFactAggEntry implements Serializable {

    String conceptCode

    String valueType
    String textValue
    BigDecimal numberValue
    String valueFlag

    static belongsTo = [
            patient: PatientDimension,
    ]

    static mapping = {
        table name: 'aggregated_observation_facts_view', schema: 'BIOMART_USER'

        id composite: ['patient', 'conceptCode']

        conceptCode column: 'concept_cd'
        patient column: 'patient_num'
        valueType column: 'valtype_cd'
        textValue column: 'tval_char'
        numberValue column: 'nval_num'
        version false
    }

    static constraints = {
        patient nullable: true
        conceptCode maxSize: 50
        valueType nullable: true, maxSize: 50
        textValue nullable: true
        numberValue nullable: true, scale: 5
        valueFlag nullable: true, maxSize: 50
    }
}
