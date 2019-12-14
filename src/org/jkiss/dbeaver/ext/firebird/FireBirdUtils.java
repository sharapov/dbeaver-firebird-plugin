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
package org.jkiss.dbeaver.ext.firebird;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.firebird.model.FireBirdTrigger;
import org.jkiss.dbeaver.ext.firebird.model.FireBirdTriggerType;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedureParameter;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureParameterKind;
import org.jkiss.utils.CommonUtils;
import org.osgi.framework.Version;

import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jkiss.dbeaver.ext.firebird.model.FirebirdGenericProcedure;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;

/**
 * FireBird utils
 */
public class FireBirdUtils {

    private static final Log LOG = Log.getLog(FireBirdUtils.class);

    public static String getProcedureSource(DBRProgressMonitor monitor, GenericProcedure procedure)
            throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, procedure, "Load procedure source code")) {
            DatabaseMetaData fbMetaData = session.getOriginal().getMetaData();
            String source = (String) fbMetaData.getClass().getMethod("getProcedureSourceCode", String.class).invoke(fbMetaData, procedure.getName());
            if (CommonUtils.isEmpty(source)) {
                return null;
            }

            return getProcedureSourceWithHeader(monitor, procedure, source);
        } catch (SQLException e) {
            throw new DBException("Can't read source code of procedure '" + procedure.getName() + "'", e);
        } catch (Exception e) {
            LOG.debug(e);
            return null;
        }
    }

    public static String getFunctionSource(DBRProgressMonitor monitor, FirebirdGenericProcedure function)
            throws DBException {
        return getFunctionSourceWithHeader(monitor, function, function.getSource());
        /*try (JDBCSession session = DBUtils.openMetaSession(monitor, function, "Load function source code")) {
            DatabaseMetaData fbMetaData = session.getOriginal().getMetaData();
            String source = (String) fbMetaData.getClass().getMethod("getProcedureSourceCode", String.class).invoke(fbMetaData, function.getName());
            if (CommonUtils.isEmpty(source)) {
                return null;
            }

            return getFunctionSourceWithHeader(monitor, function, source);
        } catch (SQLException e) {
            throw new DBException("Can't read source code of procedure '" + function.getName() + "'", e);
        } catch (Exception e) {
            log.debug(e);
            return null;
        }*/
    }

    public static String getViewSource(DBRProgressMonitor monitor, GenericTableBase view)
            throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, view, "Load view source code")) {
            DatabaseMetaData fbMetaData = session.getOriginal().getMetaData();
            String source = (String) fbMetaData.getClass().getMethod("getViewSourceCode", String.class).invoke(fbMetaData, view.getName());
            if (CommonUtils.isEmpty(source)) {
                return null;
            }

            return getViewSourceWithHeader(monitor, view, source);
        } catch (SQLException e) {
            throw new DBException("Can't read source code of view '" + view.getName() + "'", e);
        } catch (Exception e) {
            LOG.debug(e);
            return null;
        }
    }

    public static String getTriggerSource(DBRProgressMonitor monitor, FireBirdTrigger trigger)
            throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, trigger, "Load trigger source code")) {
            DatabaseMetaData fbMetaData = session.getOriginal().getMetaData();
            String source = (String) fbMetaData.getClass().getMethod("getTriggerSourceCode", String.class).invoke(fbMetaData, trigger.getName());
            if (CommonUtils.isEmpty(source)) {
                return null;
            }

            return getTriggerSourceWithHeader(monitor, trigger, source);
        } catch (SQLException e) {
            throw new DBException("Can't read source code of trigger '" + trigger.getName() + "'", e);
        } catch (Exception e) {
            LOG.debug(e);
            return null;
        }
    }

    public static String getProcedureSourceWithHeader(DBRProgressMonitor monitor, GenericProcedure procedure, String source) throws DBException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE OR ALTER PROCEDURE ").append(procedure.getName()).append(" ");
        Collection<GenericProcedureParameter> parameters = procedure.getParameters(monitor);
        if (parameters != null && !parameters.isEmpty()) {
            List<GenericProcedureParameter> args = new ArrayList<>();
            List<GenericProcedureParameter> results = new ArrayList<>();
            for (GenericProcedureParameter param : parameters) {
                if (param.getParameterKind() == DBSProcedureParameterKind.OUT || param.getParameterKind() == DBSProcedureParameterKind.RETURN) {
                    results.add(param);
                } else {
                    args.add(param);
                }
            }
            if (!args.isEmpty()) {
                sql.append("(");
                for (int i = 0; i < args.size(); i++) {
                    GenericProcedureParameter param = args.get(i);
                    if (i > 0) {
                        sql.append(", ");
                    }
                    printParam(sql, param);
                }
                sql.append(")\n");
            }
            if (!results.isEmpty()) {
                sql.append("RETURNS (\n");
                for (int i = 0; i < results.size(); i++) {
                    sql.append('\t');
                    GenericProcedureParameter param = results.get(i);
                    printParam(sql, param);
                    if (i < results.size() - 1) {
                        sql.append(",");
                    }
                    sql.append('\n');
                }
                sql.append(")\n");
            }
        }

        sql.append("AS\n").append(source);

        return sql.toString();
    }

    public static String getFunctionSourceWithHeader(JDBCSession session, DBRProgressMonitor monitor, FirebirdGenericProcedure function, String source) throws DBException, SQLException {
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlret = new StringBuilder();
        sql.append("CREATE OR ALTER FUNCTION ").append(function.getName()).append(" ");
        // Read metadata
        try (JDBCPreparedStatement dbStat = session.prepareStatement("select pp.RDB$FUNCTION_NAME, COALESCE(pp.RDB$ARGUMENT_NAME, '') RDB$ARGUMENT_NAME, pp.RDB$ARGUMENT_POSITION, pp.RDB$FIELD_TYPE,\n"
                + "       fs.rdb$field_type, fs.rdb$field_sub_type,\n"
                + "       fs.rdb$character_length, fs.rdb$field_precision, fs.rdb$field_scale,\n"
                + "       cr.rdb$character_set_name, co.rdb$collation_name,\n"
                + "       pp.rdb$description,\n"
                + "       pp.RDB$DEFAULT_SOURCE, fs.RDB$DEFAULT_SOURCE,\n"
                + "       iif(left(pp.rdb$field_source, 4) = 'RDB$', NULL, rdb$field_source) FLD_DOMAIN, -- domain\n"
                + "       pp.rdb$null_flag, -- Is NULL\n"
                + "       pp.rdb$relation_name tf_relation_name, pp.rdb$field_name tf_field_name,\n"
                + "     CASE WHEN pp.rdb$relation_name IS NULL THEN "
                + "      case fs.rdb$field_type\n"
                + "        when 7 then 'smallint'\n"
                + "        when 8 then 'integer'\n"
                + "        when 10 then 'float'\n"
                + "        when 14 then 'char'\n"
                + "        when 16 then -- только диалект 3\n"
                + "          case fs.rdb$field_sub_type\n"
                + "            when 0 then 'bigint'\n"
                + "            when 1 then 'numeric'\n"
                + "            when 2 then 'decimal'\n"
                + "            else 'unknown'\n"
                + "          end\n"
                + "        when 12 then 'date'\n"
                + "        when 13 then 'time'\n"
                + "      when 27 then -- только диалект 1\n"
                + "        case fs.rdb$field_scale\n"
                + "          when 0 then 'double precision'\n"
                + "          else 'numeric'\n"
                + "        end\n"
                + "      when 35 then 'date'  --или timestamp в зависимости от диалекта\n"
                + "      when 37 then 'varchar'\n"
                + "      when 261 then 'blob'\n"
                + "      when 14 then 'varchar'\n"
                + "      when 23 then 'boolean'\n "
                + "      when 37 then 'varchar'        \n"
                + "      else 'unknown'\n"
                + "      end\n"
                + "      ELSE 'TYPE OF COLUMN '||TRIM(pp.rdb$relation_name)||'.'||TRIM(pp.rdb$field_name) "
                + "      END "
                + "       argtype\n"
                + "from RDB$FUNCTION_ARGUMENTS pp\n"
                + "  left join rdb$fields fs on fs.rdb$field_name = pp.rdb$field_source\n"
                + "  left join rdb$character_sets cr\n"
                + "                on fs.rdb$character_set_id = cr.rdb$character_set_id\n"
                + "  left join rdb$collations co\n"
                + "               on ((pp.rdb$collation_id = co.rdb$collation_id) and\n"
                + "                   (fs.rdb$character_set_id = co.rdb$character_set_id))\n"
                + "where pp.RDB$PACKAGE_NAME IS NULL AND pp.RDB$FUNCTION_NAME = ? \n"
                + "order by pp.RDB$FIELD_TYPE, pp.RDB$ARGUMENT_POSITION"
        /*"SELECT RDB$FUNCTION_NAME, coalesce(RDB$ARGUMENT_NAME,'$$$') RDB$ARGUMENT_NAME, RDB$ARGUMENT_POSITION, coalesce(RDB$TYPE_NAME,'$$$') RDB$TYPE_NAME, RDB$TYPE "
                    + "FROM RDB$FUNCTION_ARGUMENTS, RDB$TYPES, RDB$FIELDS WHERE RDB$PACKAGE_NAME IS NULL AND RDB$FUNCTION_NAME = ? " 
                    + "AND RDB$TYPE=RDB$FIELDS.RDB$FIELD_TYPE AND RDB$FIELD_SOURCE=RDB$FIELDS.RDB$FIELD_NAME AND RDB$TYPES.RDB$FIELD_NAME='RDB$FIELD_TYPE' " 
                    + "AND RDB$PACKAGE_NAME IS null " 
                    + "ORDER BY RDB$ARGUMENT_POSITION"*/
        )) {
            dbStat.setString(1, function.getName());
            //dbStat.setString(2, getName());
            String argname;
            String argtype;
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.next()) {
                    argname = JDBCUtils.safeGetString(dbResult, "RDB$ARGUMENT_NAME");
                    argtype = JDBCUtils.safeGetString(dbResult, "argtype");
                    if (JDBCUtils.safeGetInt(dbResult, "RDB$ARGUMENT_POSITION") == 0) {
                        sqlret.append(" RETURNS ").append(argname == null ? "" : argname.trim()).append(" ").append(argtype == null ? "" : argtype.trim())
                                .append((function.isDeterministic() ? " DETERMINISTIC" : " "));
                        sql.append("(");
                    } else {
                        sql.append("(").append(argname == null ? "" : argname.trim()).append(" ").append(argtype == null ? "" : argtype.trim());
                    }
                    while (dbResult.next()) {
	                    argname = JDBCUtils.safeGetString(dbResult, "RDB$ARGUMENT_NAME");
        	            argtype = JDBCUtils.safeGetString(dbResult, "argtype");
                        sql.append((JDBCUtils.safeGetInt(dbResult, "RDB$ARGUMENT_POSITION") != 1 ? ",\n" : "\n")).append(argname == null ? "" : argname.trim()).append(" ").append(argtype == null ? "" : argtype.trim());
                    }
                    sql.append(")\n ").append(sqlret);
                }
            }
        }
        sql.append("\n AS\n").append(source);
        if (function.getDescription() != null) {
            sql.append("\n\nCOMMENT ON FUNCTION " + function.getName() + " IS '" + function.getDescription() + "'\n");
        }
        return sql.toString();
    }

    public static String getFunctionSourceWithHeader(DBRProgressMonitor monitor, FirebirdGenericProcedure function, String source) throws DBException {
        String ret = "";
        try (JDBCSession session = DBUtils.openMetaSession(monitor, function.getDataSource(), "Read column domain type")) {
            ret = getFunctionSourceWithHeader(session, monitor, function, source);
        } catch (SQLException ex) {
            throw new DBException("Error reading column domain type", ex);
        }
        //Collection<GenericProcedureParameter> parameters = function.getParameters(monitor);
        /*if (parameters != null && !parameters.isEmpty()) {
            List<GenericProcedureParameter> args = new ArrayList<>();
            List<GenericProcedureParameter> results = new ArrayList<>();
            for (GenericProcedureParameter param : parameters) {
                if (param.getParameterKind() == DBSProcedureParameterKind.OUT || param.getParameterKind() == DBSProcedureParameterKind.RETURN) {
                    results.add(param);
                } else {
                    args.add(param);
                }
            }
            if (!args.isEmpty()) {
                sql.append("(");
                for (int i = 0; i < args.size(); i++) {
                    GenericProcedureParameter param = args.get(i);
                    if (i > 0) sql.append(", ");
                    printParam(sql, param);
                }
                sql.append(")\n");
            }
            if (!results.isEmpty()) {
                sql.append("RETURNS (\n");
                for (int i = 0; i < results.size(); i++) {
                    sql.append('\t');
                    GenericProcedureParameter param = results.get(i);
                    printParam(sql, param);
                    if (i < results.size() - 1) sql.append(",");
                    sql.append('\n');
                }
                sql.append(")\n");
            }
        }*/

        return ret;
    }

    private static void printParam(StringBuilder sql, GenericProcedureParameter param) {
        sql.append(DBUtils.getQuotedIdentifier(param)).append(" ").append(param.getTypeName());
        if (param.getDataKind() == DBPDataKind.STRING) {
            sql.append("(").append(param.getMaxLength()).append(")");
        }
    }

    public static String getViewSourceWithHeader(DBRProgressMonitor monitor, GenericTableBase view, String source) throws DBException {
        Version version = getFireBirdServerVersion(view.getDataSource());
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE ");
        if (version.getMajor() > 2 || (version.getMajor() == 2 && version.getMinor() >= 5)) {
            sql.append("OR ALTER ");
        }
        sql.append("VIEW ").append(view.getName()).append(" ");
        Collection<? extends GenericTableColumn> columns = view.getAttributes(monitor);
        if (columns != null) {
            sql.append("(");
            boolean first = true;
            for (GenericTableColumn column : columns) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                sql.append(DBUtils.getQuotedIdentifier(column));
            }
            sql.append(")\n");
        }
        sql.append("AS\n").append(source);

        return sql.toString();
    }

    public static String getTriggerSourceWithHeader(DBRProgressMonitor monitor, FireBirdTrigger trigger, String source) throws DBException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TRIGGER ").append(trigger.getName()).append(" ");
        FireBirdTriggerType type = trigger.getType();
        if (type.isDbEvent()) {
            sql.append(type.getDisplayName());
        } else if (trigger.getTable() != null) {
            sql.append("FOR ").append(DBUtils.getQuotedIdentifier(trigger.getTable()));
            sql.append(" ").append(type.getDisplayName());
        }
        sql.append("\n").append(source);

        return sql.toString();
    }

    public static String getPlan(JDBCPreparedStatement statement) {
        String plan = "";
        try {
            plan = (String) statement.getOriginal().getClass().getMethod("getExecutionPlan").invoke(statement.getOriginal());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return plan;
    }

    private static final Pattern VERSION_PATTERN = Pattern.compile(".+\\-V([0-9]+\\.[0-9]+\\.[0-9]+).+");

    public static Version getFireBirdServerVersion(DBPDataSource dataSource) {
        String versionInfo = dataSource.getInfo().getDatabaseProductVersion();
        Matcher matcher = VERSION_PATTERN.matcher(versionInfo);
        if (matcher.matches()) {
            return new Version(matcher.group(1));
        }
        return new Version(0, 0, 0);
    }

}
