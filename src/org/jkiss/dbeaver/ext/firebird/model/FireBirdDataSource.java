/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2019 Serge Rider (serge@jkiss.org)
 *
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

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.firebird.model.plan.FireBirdPlanAnalyser;
import org.jkiss.dbeaver.ext.generic.model.*;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanStyle;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.IntKeyMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

public class FireBirdDataSource extends GenericDataSource
        implements DBCQueryPlanner {

    private static final Log LOG = Log.getLog(FireBirdDataSource.class);

    private static class MetaFieldInfo {

        int type;
        String name;
        String description;

        MetaFieldInfo(int type, String name, String description) {
            this.type = type;
            this.name = name;
            this.description = description;
        }

        @Override
        public String toString() {
            return name + ":" + type;
        }
    }

    private final Map<String, IntKeyMap<MetaFieldInfo>> metaFields = new HashMap<>();

    public FireBirdDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container, GenericMetaModel metaModel)
            throws DBException {
        super(monitor, container, metaModel, new FireBirdSQLDialect());
    }

    public String getMetaFieldValue(String name, int type) {
        IntKeyMap<MetaFieldInfo> fieldMap = metaFields.get(name);
        if (fieldMap != null) {
            MetaFieldInfo info = fieldMap.get(type);
            if (info != null) {
                return info.name;
            }
        }
        return null;
    }

    @Override
    public void initialize(DBRProgressMonitor monitor) throws DBException {
        // Read metadata
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read generic metadata")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT * FROM RDB$TYPES")) {
                monitor.subTask("Load FireBird types");
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        if (monitor.isCanceled()) {
                            break;
                        }
                        String fieldName = JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME");
                        if (fieldName == null) {
                            continue;
                        }
                        fieldName = fieldName.trim();
                        int fieldType = JDBCUtils.safeGetInt(dbResult, "RDB$TYPE");
                        String typeName = JDBCUtils.safeGetString(dbResult, "RDB$TYPE_NAME");
                        if (typeName == null) {
                            continue;
                        }
                        typeName = typeName.trim();
                        String fieldDescription = JDBCUtils.safeGetString(dbResult, "RDB$SYSTEM_FLAG");
                        IntKeyMap<MetaFieldInfo> metaFields = this.metaFields.get(fieldName);
                        if (metaFields == null) {
                            metaFields = new IntKeyMap<>();
                            this.metaFields.put(fieldName, metaFields);
                        }
                        metaFields.put(fieldType, new MetaFieldInfo(fieldType, typeName, fieldDescription));
                    }
                }
            }

        } catch (SQLException ex) {
            LOG.error("Error reading FB metadata", ex);
        }

        // Init
        super.initialize(monitor);
    }

    /*@Override
    public Collection<GenericProcedure> getProcedures(DBRProgressMonitor monitor, String name) throws DBException {
        return getProcedures(monitor); //To change body of generated methods, choose Tools | Templates.
    }
    
    

    @Override
    public Collection<? extends GenericProcedure> getProceduresOnly(DBRProgressMonitor monitor) throws DBException {
        return getProcedures(monitor); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Collection<GenericProcedure> getProcedures(DBRProgressMonitor monitor) throws DBException {
        ArrayList<GenericProcedure> result = new ArrayList<>();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read column domain type")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + " "
                    + "RDB$FUNCTION_NAME,RDB$FUNCTION_TYPE,RDB$QUERY_NAME,RDB$DESCRIPTION,RDB$MODULE_NAME,RDB$ENTRYPOINT,RDB$RETURN_ARGUMENT,RDB$SYSTEM_FLAG,RDB$ENGINE_NAME,RDB$PACKAGE_NAME,RDB$PRIVATE_FLAG,RDB$FUNCTION_SOURCE,RDB$FUNCTION_ID,RDB$FUNCTION_BLR,RDB$VALID_BLR,RDB$DEBUG_INFO,RDB$SECURITY_CLASS,RDB$OWNER_NAME,RDB$LEGACY_FLAG,RDB$DETERMINISTIC_FLAG "
                    + " FROM RDB$FUNCTIONS")) {
                //dbStat.setString(1, getTable().getName());
                //dbStat.setString(2, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.next()) {
                        result.add(new GenericProcedure(this, JDBCUtils.safeGetString(dbResult, 1), "spec", "descr", DBSProcedureType.FUNCTION, GenericFunctionResultType.NO_TABLE));
                    }
                }

            } catch (SQLException ex) {
                throw new DBException("Error reading column domain type", ex);
            }
    }
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read column domain type")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + " "
                    + "RDB$PROCEDURE_NAME,RDB$PROCEDURE_ID,RDB$PROCEDURE_INPUTS,RDB$PROCEDURE_OUTPUTS,RDB$DESCRIPTION,RDB$PROCEDURE_SOURCE,RDB$PROCEDURE_BLR,RDB$SECURITY_CLASS,RDB$OWNER_NAME,RDB$RUNTIME,RDB$SYSTEM_FLAG,RDB$PROCEDURE_TYPE,RDB$VALID_BLR,RDB$DEBUG_INFO,RDB$ENGINE_NAME,RDB$ENTRYPOINT,RDB$PACKAGE_NAME,RDB$PRIVATE_FLAG "
                    + " FROM RDB$PROCEDURES")) {
                //dbStat.setString(1, getTable().getName());
                //dbStat.setString(2, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.next()) {
                        result.add(new GenericProcedure(this, JDBCUtils.safeGetString(dbResult, 1), "specp", "descrp", DBSProcedureType.PROCEDURE, GenericFunctionResultType.NO_TABLE));
                    }
                }

            } catch (SQLException ex) {
                throw new DBException("Error reading column domain type", ex);
            }
    }
        return result;
    }*/
    @Override
    public Collection<GenericPackage> getPackages(DBRProgressMonitor monitor)
            throws DBException {
        ArrayList<GenericPackage> result = new ArrayList<>();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read column domain type")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT RDB$PACKAGE_NAME,RDB$PACKAGE_HEADER_SOURCE,RDB$PACKAGE_BODY_SOURCE,RDB$VALID_BODY_FLAG,RDB$SECURITY_CLASS,RDB$OWNER_NAME,RDB$SYSTEM_FLAG,RDB$DESCRIPTION FROM RDB$PACKAGES")) {
                //dbStat.setString(1, getTable().getName());
                //dbStat.setString(2, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.next()) {
                        GenericPackage gp = new GenericPackage(this, JDBCUtils.safeGetString(dbResult, 1).trim() /*"PACKAGES"*/, false);
                        result.add(gp);
                        //GenericPackage gpc  = new GenericPackage(this, JDBCUtils.safeGetString(dbResult, 1).trim(), false);
                        //gp.addPackage(gpc);
                        GenericProcedure gpp = new GenericProcedure(gp, /*JDBCUtils.safeGetString(dbResult, 1).trim() + */ "HEADER", JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_HEADER_SOURCE") + "\n" + JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_BODY_SOURCE"), JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION"), DBSProcedureType.UNKNOWN, GenericFunctionResultType.UNKNOWN);
                        gpp.setSource(
                                "CREATE OR ALTER PACKAGE " + JDBCUtils.safeGetString(dbResult, 1).trim() + " \nAS\n"
                                + JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_HEADER_SOURCE") + ";\n\n"/*\n\nRECREATE PACKAGE BODY "+JDBCUtils.safeGetString(dbResult, 1).trim()+"\n" +
                            "AS\n"+JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_BODY_SOURCE")+";\n\n"*/
                                + "COMMENT ON PACKAGE " + JDBCUtils.safeGetString(dbResult, 1).trim() + " IS '" + JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION") + "';\n"
                        );
                        gp.addProcedure(gpp);
                        GenericProcedure gpp1 = new GenericProcedure(gp, /*JDBCUtils.safeGetString(dbResult, 1).trim()+*/ "BODY",
                                /*JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_HEADER_SOURCE")+"\n"+JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_BODY_SOURCE")*/ "", JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION"), DBSProcedureType.UNKNOWN, GenericFunctionResultType.UNKNOWN);
                        gpp1.setSource(
                                /*"CREATE OR ALTER PACKAGE "+JDBCUtils.safeGetString(dbResult, 1).trim()+" \nAS\n" 
                                + JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_HEADER_SOURCE")+";\n\n"+*/"RECREATE PACKAGE BODY " + JDBCUtils.safeGetString(dbResult, 1).trim() + "\n"
                                + "AS\n" + JDBCUtils.safeGetString(dbResult, "RDB$PACKAGE_BODY_SOURCE") + ";\n\n"
                        //+ "COMMENT ON PACKAGE "+JDBCUtils.safeGetString(dbResult, 1).trim()+" IS '"+JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION")+"';\n"
                        );
                        gp.addProcedure(gpp1);
                        //result.add(gpc);
                    }
                }

            } catch (SQLException ex) {
                throw new DBException("Error reading column domain type", ex);
            }
        }

        //return structureContainer == null ? null : structureContainer.getPackages(monitor);
        return result;
    }

    @NotNull
    @Override
    public FireBirdDataSource getDataSource() {
        return this;
    }

    @Override
    public DBCPlan planQueryExecution(DBCSession session, String query) throws DBException {
        FireBirdPlanAnalyser plan = new FireBirdPlanAnalyser(this, (JDBCSession) session, query);
        plan.explain();
        return plan;
    }

    @Override
    public DBCPlanStyle getPlanStyle() {
        return DBCPlanStyle.PLAN;
    }
}
