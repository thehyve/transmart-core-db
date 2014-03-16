package org.transmartproject.db

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.everyItem
import static org.hamcrest.Matchers.isA

class BaseTestData {

    static void save(List objects) {
        if (objects == null) {
            return //shortcut for no objects to save
        }

        List result = objects*.save()
        result.eachWithIndex { def entry, int i ->
            if (entry == null) {
                throw new RuntimeException("Could not save ${objects[i]}. Errors: ${objects[i].errors}")
            }
        }

        assertThat result, everyItem(isA(objects[0].getClass()))
    }

    void saveAll() {
        //nothing to be saved by default, should be overriden
        //this should be an abstract method and class, but if i make this class abstract,
        //MarshallerRegistrarServiceTests will fail with java.lang.StackOverflowError
    }

}
