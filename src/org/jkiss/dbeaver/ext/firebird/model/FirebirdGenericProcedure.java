/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.firebird.model;

import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

/**
 *
 * @author a.v.sharapov
 */
public class FirebirdGenericProcedure extends GenericProcedure {

    private boolean deterministic = false;

    public FirebirdGenericProcedure(GenericStructContainer container, String procedureName, String specificName, String description, DBSProcedureType procedureType, GenericFunctionResultType functionResultType, boolean deterministic) {
        super(container, procedureName, specificName, description, procedureType, functionResultType);
        this.deterministic = deterministic;
    }

    public FirebirdGenericProcedure(GenericStructContainer container, String procedureName, String specificName, String description, DBSProcedureType procedureType, GenericFunctionResultType functionResultType) {
        super(container, procedureName, specificName, description, procedureType, functionResultType);
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

}
