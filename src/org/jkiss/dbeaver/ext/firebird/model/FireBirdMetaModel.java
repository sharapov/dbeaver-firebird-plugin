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
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.firebird.FireBirdUtils;
import org.jkiss.dbeaver.ext.generic.model.*;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPErrorAssistant;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jkiss.dbeaver.Log;
import static org.jkiss.dbeaver.ext.firebird.FireBirdUtils.getFunctionSourceWithHeader;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

/**
 * FireBirdMetaModel
 */
public class FireBirdMetaModel extends GenericMetaModel {

    private final Pattern ERROR_POSITION_PATTERN = Pattern.compile(" line ([0-9]+), column ([0-9]+)");
    private static final Log LOG = Log.getLog(FireBirdDataSource.class);

    public FireBirdMetaModel() {
        super();
    }

    @Override
    public GenericDataSource createDataSourceImpl(DBRProgressMonitor monitor, DBPDataSourceContainer container) throws DBException {
        return new FireBirdDataSource(monitor, container, this);
    }

    @Override
    public FireBirdDataTypeCache createDataTypeCache(@NotNull GenericStructContainer container) {
        return new FireBirdDataTypeCache(container);
    }

    @Override
    public String getViewDDL(DBRProgressMonitor monitor, GenericView sourceObject, Map<String, Object> options) throws DBException {
        return FireBirdUtils.getViewSource(monitor, sourceObject);
    }

    @Override
    public String getProcedureDDL(DBRProgressMonitor monitor, GenericProcedure sourceObject) throws DBException {
        //Log.getLog(this.getClass()).info(">>>>>>>>>>>>>>>>>>!!!!!!!!!"+sourceObject.getName()+"!!!!!!!!!!!!!!!!!!!<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        return (sourceObject.getProcedureType() == DBSProcedureType.FUNCTION ? FireBirdUtils.getFunctionSource(monitor, (FirebirdGenericProcedure) sourceObject) : FireBirdUtils.getProcedureSource(monitor, sourceObject));
    }

    @Override
    public boolean supportsSequences(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public List<GenericSequence> loadSequences(@NotNull DBRProgressMonitor monitor, @NotNull GenericStructContainer container) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Read sequences")) {
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT * FROM RDB$GENERATORS")) {
                List<GenericSequence> result = new ArrayList<>();

                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String name = JDBCUtils.safeGetString(dbResult, "RDB$GENERATOR_NAME");
                        if (name == null) {
                            continue;
                        }
                        String description = JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION");
                        FireBirdSequence sequence = new FireBirdSequence(
                                container,
                                name,
                                description,
                                null,
                                0,
                                -1,
                                1
                        );
                        result.add(sequence);
                    }
                }

                return result;
            }
        } catch (SQLException e) {
            throw new DBException(e, container.getDataSource());
        }
    }

    @Override
    public boolean supportsTriggers(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public boolean supportsDatabaseTriggers(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public List<GenericTrigger> loadTriggers(DBRProgressMonitor monitor, @NotNull GenericStructContainer container, @Nullable GenericTableBase table) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Read triggers")) {
            try (JDBCPreparedStatement dbStat = session.prepareStatement(
                    "SELECT * FROM RDB$TRIGGERS\n"
                    + "WHERE RDB$RELATION_NAME" + (table == null ? " IS NULL" : "=?"))) {
                if (table != null) {
                    dbStat.setString(1, table.getName());
                }
                List<GenericTrigger> result = new ArrayList<>();

                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String name = JDBCUtils.safeGetString(dbResult, "RDB$TRIGGER_NAME");
                        if (name == null) {
                            continue;
                        }
                        name = name.trim();
                        int sequence = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_SEQUENCE");
                        int type = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_TYPE");
                        String description = JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION");
                        FireBirdTrigger trigger = new FireBirdTrigger(
                                container,
                                table,
                                name,
                                description,
                                FireBirdTriggerType.getByType(type),
                                sequence);
                        result.add(trigger);
                    }
                }
                return result;

            }
        } catch (SQLException e) {
            throw new DBException(e, container.getDataSource());
        }
    }

    @Override
    public String getTriggerDDL(@NotNull DBRProgressMonitor monitor, @NotNull GenericTrigger trigger) throws DBException {
        return FireBirdUtils.getTriggerSource(monitor, (FireBirdTrigger) trigger);
    }

    @Override
    public DBPErrorAssistant.ErrorPosition getErrorPosition(@NotNull Throwable error) {
        String message = error.getMessage();
        if (!CommonUtils.isEmpty(message)) {
            Matcher matcher = ERROR_POSITION_PATTERN.matcher(message);
            if (matcher.find()) {
                DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                pos.line = Integer.parseInt(matcher.group(1)) - 1;
                pos.position = Integer.parseInt(matcher.group(2)) - 1;
                return pos;
            }
        }
        return null;
    }

    @Override
    public boolean isSystemTable(GenericTableBase table) {
        String tableName = table.getName();
        tableName = tableName.toUpperCase(Locale.ENGLISH);
        return tableName.startsWith("RDB$") || tableName.startsWith("MON$");    // [JDBC: Firebird]
    }

    @Override
    public GenericTableBase createTableImpl(GenericStructContainer container, String tableName, String tableType, JDBCResultSet dbResult) {
        if (tableType != null && isView(tableType)) {
            return new FireBirdView(container, tableName, tableType, dbResult);
        } else {
            return new FireBirdTable(container, tableName, tableType, dbResult);
        }
    }

    @Override
    public GenericTableColumn createTableColumnImpl(DBRProgressMonitor monitor, GenericTableBase table, String columnName, String typeName, int valueType, int sourceType, int ordinalPos, long columnSize, long charLength, Integer scale, Integer precision, int radix, boolean notNull, String remarks, String defaultValue, boolean autoIncrement, boolean autoGenerated) throws DBException {
        return new FireBirdTableColumn(monitor, table,
                columnName,
                typeName, valueType, sourceType, ordinalPos,
                columnSize,
                charLength, scale, precision, radix, notNull,
                remarks, defaultValue, autoIncrement, autoGenerated
        );
    }

    @Override
    public void loadProcedures(DBRProgressMonitor dbrpm, GenericObjectContainer goc) throws DBException {
        //LOG.info("loadProcedures ===========================================");
        super.loadProcedures(dbrpm, goc);
        try (JDBCSession session = DBUtils.openMetaSession(dbrpm, goc, "Read column domain type")) {
            HashMap<String, FirebirdGenericProcedure> fl = new HashMap<>();
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + " "
                    + "RDB$FUNCTION_NAME,RDB$FUNCTION_TYPE,RDB$QUERY_NAME,RDB$DESCRIPTION,RDB$MODULE_NAME,RDB$ENTRYPOINT,RDB$RETURN_ARGUMENT,RDB$SYSTEM_FLAG,RDB$ENGINE_NAME,RDB$PACKAGE_NAME,RDB$PRIVATE_FLAG,RDB$FUNCTION_SOURCE,RDB$FUNCTION_ID,RDB$FUNCTION_BLR,RDB$VALID_BLR,RDB$DEBUG_INFO,RDB$SECURITY_CLASS,RDB$OWNER_NAME,RDB$LEGACY_FLAG,RDB$DETERMINISTIC_FLAG "
                    + " FROM RDB$FUNCTIONS WHERE RDB$PACKAGE_NAME is null")) {
                //dbStat.setString(1, getTable().getName());
                //dbStat.setString(2, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        //result.add(new GenericProcedure(this, JDBCUtils.safeGetString(dbResult, 1), "spec", "descr", DBSProcedureType.FUNCTION, GenericFunctionResultType.NO_TABLE));
                        //final GenericProcedure procedure = createProcedureImpl(
                        String fname = JDBCUtils.safeGetString(dbResult, "RDB$FUNCTION_NAME");
                        final FirebirdGenericProcedure procedure = new FirebirdGenericProcedure(
                                goc,
                                (fname != null ? fname.trim() : ""),
                                null,
                                null,
                                DBSProcedureType.FUNCTION,
                                GenericFunctionResultType.NO_TABLE,
                                (JDBCUtils.safeGetInt(dbResult, "RDB$DETERMINISTIC_FLAG") == 1)
                        );
                        procedure.setDescription(JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION"));
                        procedure.setSource(JDBCUtils.safeGetString(dbResult, "RDB$FUNCTION_SOURCE"));
                        //procedure.setPersisted(JDBCUtils.safeGetInt(dbResult, "RDB$DETERMINISTIC_FLAG")==1);
                        //final GenericMetaObject pcObject = procedure.getDataSource().getMetaObject(GenericConstants.OBJECT_PROCEDURE_COLUMN);
                        fl.put(procedure.getName(), procedure);
                        //Log.getLog(this.getClass()).info("procedure.getParameters(dbrpm)="+procedure.getParameters(dbrpm));
                        //procedure.getParameters(dbrpm).add(new GenericProcedureParameter(procedure, "i", "int", Types.INTEGER, 1, 1, 
                        //Integer.SIZE, Integer.SIZE, true, "remarks", DBSProcedureParameterKind.RETURN));
                        //for (GenericProcedure g : procedure.getContainer().getProcedures(dbrpm,JDBCUtils.safeGetString(dbResult, "RDB$FUNCTION_NAME").trim())) {
                        //    Log.getLog(this.getClass()).info(g.getName());
                        //}
                        //Log.getLog(this.getClass()).info("dbResult.isClosed()="+dbResult.isClosed());
                    }
                }
            } catch (SQLException ex) {
                throw new DBException("Error reading packages", ex);
            }

            if (!session.getMetaData().getDriverVersion().equals("3.0")) {
                for (GenericProcedure gp : goc.getFunctionsOnly(dbrpm)) {
                    //Log.getLog(this.getClass()).info(gp.getParentObject().getName()+"!!!!!!!!!!!!!!!!!!!!!= "+gp.getName());
                    gp.setSource(getFunctionSourceWithHeader(session, dbrpm, fl.get(gp.getName()), fl.get(gp.getName()).getSource()));
                }
                return;
            }
            for (FirebirdGenericProcedure gp : fl.values()) {
                gp.setSource(getFunctionSourceWithHeader(session, dbrpm, fl.get(gp.getName()), fl.get(gp.getName()).getSource()));
                goc.addProcedure(gp);
                //gp.getParameters(dbrpm).add(new GenericProcedureParameter(gp, 
                //      "wwww", "integer", 0, 0, 0, Integer.SIZE, Integer.SIZE, true, "", 
                //    DBSProcedureParameterKind.RETURN));
                //Log.getLog(this.getClass()).info("!!!!!!!!!!!!!!!!!!!!!gp.getName()="+gp.getName()+" "+gp.getSource());
                //gp.setSource(getFunctionSourceWithHeader(session, dbrpm, gp, gp.getSource()));
                //for(GenericProcedureParameter gpp:gp.getParameters(dbrpm)){
                //    Log.getLog(this.getClass()).info(gpp.getParentObject().getName()+" "+gpp.getName());
            }
            //goc.addProcedure(gp);
            //Log.getLog(this.getClass()).info("!!!!!!!!!!!!!!!!!!!!!gp.getName()="+gp.getName()+" "+gp.getSource());

            //}
        } catch (SQLException ex) {
            throw new DBException("Error reading column domain type", ex);
        }
        /*try (JDBCSession session = DBUtils.openMetaSession(dbrpm, goc, "Read column domain type")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + " "
                    + "RDB$PROCEDURE_NAME,RDB$PROCEDURE_ID,RDB$PROCEDURE_INPUTS,RDB$PROCEDURE_OUTPUTS,RDB$DESCRIPTION,RDB$PROCEDURE_SOURCE,RDB$PROCEDURE_BLR,RDB$SECURITY_CLASS,RDB$OWNER_NAME,RDB$RUNTIME,RDB$SYSTEM_FLAG,RDB$PROCEDURE_TYPE,RDB$VALID_BLR,RDB$DEBUG_INFO,RDB$ENGINE_NAME,RDB$ENTRYPOINT,RDB$PACKAGE_NAME,RDB$PRIVATE_FLAG "
                    + " FROM RDB$PROCEDURES WHERE RDB$PACKAGE_NAME is null")) {
                //dbStat.setString(1, getTable().getName());
                //dbStat.setString(2, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.next()) {
                        //result.add(new GenericProcedure(this, JDBCUtils.safeGetString(dbResult, 1), "specp", "descrp", DBSProcedureType.PROCEDURE, GenericFunctionResultType.NO_TABLE));
                        final GenericProcedure procedure = createProcedureImpl(
                                goc,
                                JDBCUtils.safeGetString(dbResult, "RDB$PROCEDURE_NAME"),
                                null,
                                null,
                                DBSProcedureType.PROCEDURE,
                                null);
                        procedure.setSource(JDBCUtils.safeGetString(dbResult, "RDB$PROCEDURE_SOURCE"));
                        procedure.setDescription(JDBCUtils.safeGetString(dbResult, "RDB$DESCRIPTION"));
                        goc.addProcedure(procedure);
                    }
                }

            } catch (SQLException ex) {
                throw new DBException("Error reading column domain type", ex);
            }
        }*/
    }

}
