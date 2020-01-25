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

import java.sql.SQLException;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericTableForeignKey;
import org.jkiss.dbeaver.ext.generic.model.GenericTableForeignKeyColumnTable;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;

public class FireBirdTable extends GenericTable implements DBPNamedObject2 {

    private static final Log LOG = Log.getLog(FireBirdDataSource.class);

    public FireBirdTable(GenericStructContainer container, @Nullable String tableName, @Nullable String tableType, @Nullable JDBCResultSet dbResult) {
        super(container, tableName, tableType, dbResult);
    }

    @Override
    protected boolean isTruncateSupported() {
        return false;
    }

    @Override
    public synchronized List<FireBirdTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor) throws DBException {
        Collection<? extends GenericTableColumn> childColumns = super.getAttributes(monitor);
        if (childColumns == null) {
            return Collections.emptyList();
        }
        List<FireBirdTableColumn> columns = new ArrayList<>();
        for (GenericTableColumn gtc : childColumns) {
            columns.add((FireBirdTableColumn) gtc);
        }
        columns.sort(DBUtils.orderComparator());
        return columns;
    }

    public Collection<GenericTableForeignKey> getUsedBy(DBRProgressMonitor dbrpm) throws DBException {
        Collection<GenericTableForeignKey> usedBy = new ArrayList<>();
        String sql = "SELECT RDB$DEPENDENT_NAME, RDB$DEPENDED_ON_NAME, RDB$FIELD_NAME, "
                + "	CASE RDB$DEPENDENT_TYPE WHEN 0 THEN 'TABLE'"
                + "	WHEN 1 THEN 'VIEW'"
                + "	WHEN 2 THEN 'TRIGGER'"
                + "	WHEN 3 THEN 'COMPUTED COLUMN'"
                + "	WHEN 4 THEN 'CHECK'"
                + "	WHEN 5 THEN 'PROCEDURE'"
                + "	WHEN 6 THEN 'INDEX EXPRESSION'"
                + "	WHEN 9 THEN 'COLUMN'"
                + "	WHEN 15 THEN 'FUNCTION'"
                + "	WHEN 18 THEN 'PACKAGE HEAD'"
                + "	WHEN 19 THEN 'PACKAGE BODY'"
                + "	ELSE 'unknown' "
                + "END RDB$DEPENDENT_TYPE, "
                + " CASE RDB$DEPENDED_ON_TYPE WHEN 0 THEN 'TABLE'"
                + "	WHEN 1 THEN 'VIEW'"
                + "	WHEN 2 THEN 'TRIGGER'"
                + "	WHEN 3 THEN 'COMPUTED COLUMN'"
                + "	WHEN 4 THEN 'CHECK'"
                + "	WHEN 5 THEN 'PROCEDURE'"
                + "	WHEN 6 THEN 'INDEX EXPRESSION'"
                + "	WHEN 7 THEN 'EXCEPTION'"
                + "	WHEN 8 THEN 'USER'"
                + "	WHEN 9 THEN 'COLUMN'"
                + "	WHEN 10 THEN 'INDEX'"
                + "	WHEN 14 THEN 'GENERATOR'"
                + "	WHEN 15 THEN 'FUNCTION'"
                + "	WHEN 17 THEN 'SORT'"
                + "	WHEN 18 THEN 'PACKAGE HEAD'"
                + "	WHEN 19 THEN 'PACKAGE BODY'"
                + "	ELSE 'unknown' "
                + "END RDB$DEPENDED_ON_TYPE, "
                + "RDB$PACKAGE_NAME "
                + "FROM RDB$DEPENDENCIES WHERE RDB$DEPENDED_ON_NAME = ?";
        GenericTableForeignKey fk;
        try (JDBCSession session = DBUtils.openMetaSession(dbrpm, this, "Read column domain type")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement(sql)) {
                dbStat.setString(1, getName());
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        fk = new GenericTableForeignKey(this, JDBCUtils.safeGetString(dbResult, "RDB$DEPENDENT_NAME"),
                                JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME"), null, null, null, null, true);
                        fk.setConstraintType(DBSEntityConstraintType.ASSOCIATION);
                        fk.addColumn(new GenericTableForeignKeyColumnTable(fk,
                                //new GenericTableColumn(this),
                                this.getAttribute(dbrpm, JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME")) /*new GenericTableColumn(this,
                                        JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME"),
                                        JDBCUtils.safeGetString(dbResult, "RDB$DEPENDENT_TYPE"),
                                        0, 0, 0, 0, 0, 0, 0, 0, false, "ss", "ww", false, false)*/,
                                 0, this.getAttribute(dbrpm, JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME"))
                        /*new GenericTableColumn(this,
                                        JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME"),
                                        JDBCUtils.safeGetString(dbResult, "RDB$DEPENDENT_TYPE"),
                                        0, 0, 0, 0, 0, 0, 0, 0, false, "ss", "ww", false, false)*/));
                        usedBy.add(fk);
                    }
                }
            } catch (SQLException ex) {
                throw new DBException("Error reading column domain type", ex);
            }

        } //catch (SQLException ex) {
        //throw new DBException("Error reading column domain type", ex);
        //}
        //LOG.info(((GenericTableForeignKey)usedBy.toArray()[0]).getName());
        return usedBy;
    }

}
