package org.transmartproject.db.dataquery.highdim.assayconstraints

import grails.orm.HibernateCriteriaBuilder
import org.transmartproject.core.dataquery.highdim.assayconstraints.AssayConstraint
import org.transmartproject.core.exceptions.InvalidRequestException

abstract class AbstractAssayConstraint implements AssayConstraint, Serializable {

    private static final long serialVersionUID = 1L

    abstract void addConstraintsToCriteria(HibernateCriteriaBuilder builder) throws InvalidRequestException

}
