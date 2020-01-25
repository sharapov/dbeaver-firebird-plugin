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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericPackage;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

/**
 *
 * @author a.v.sharapov
 */
public class FirebirdPackage extends GenericPackage {

    private static final Log LOG = Log.getLog(FireBirdDataSource.class);

    private String source;

    public FirebirdPackage(GenericStructContainer container, String packageName, boolean nameFromCatalog) {
        super(container, packageName, nameFromCatalog);
    }

    public FirebirdPackage(GenericStructContainer container, String packageName, boolean nameFromCatalog, String source) {
        super(container, packageName, nameFromCatalog);
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public Collection<? extends GenericProcedure> getFunctionsOnly(DBRProgressMonitor monitor) throws DBException {
        //LOG.info("getFunctionsOnly");
        //LOG.info("getFunctionsOnly" + super.getFunctionsOnly(monitor).size());
        //LOG.info("getFunctionsOnly" + super.getFunctionsOnly(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.FUNCTION).collect(Collectors.toList()).size());
        return super.getProcedures(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.FUNCTION).collect(Collectors.toList());
    }

    @Override
    public List<GenericProcedure> getProcedures(DBRProgressMonitor monitor) throws DBException {
        //LOG.info("getProcedures");
        //LOG.info("getProcedures" + super.getProcedures(monitor).size());
        //LOG.info("getProcedures" + super.getProcedures(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.PROCEDURE).collect(Collectors.toList()).size());
        return super.getProcedures(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.PROCEDURE).collect(Collectors.toList());
    }

    @Override
    public List<GenericProcedure> getProcedures(DBRProgressMonitor monitor, String name) throws DBException {
        //LOG.info("getProcedures = " + name);
        //LOG.info("getProcedures" + super.getProcedures(monitor).size());
        //LOG.info("getProcedures" + super.getProcedures(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.PROCEDURE).collect(Collectors.toList()).size());
        return super.getProcedures(monitor).stream().filter(line -> line.getProcedureType() == DBSProcedureType.PROCEDURE).collect(Collectors.toList());
    }

    @Override
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        return source;//super.getObjectDefinitionText(monitor, options); 
    }

}
