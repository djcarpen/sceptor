package com.github.djcarpen.sceptor.Schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HiveTable {

        private String databaseName;
        private String communityName;
        private String appCode;
        private String moduleCode;
        private String tableName;

        private List<HiveColumn> columns = new ArrayList<>();

        public String getDatabaseName() {
            return databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }


        public List<HiveColumn> getColumns() {
            return columns;
        }

        public void setColumns(List<HiveColumn> columns) {
            this.columns = columns;
        }

        public void addColumn(HiveColumn column) {

            columns.add(column);

        }

        public String getCommunityName() {
            return communityName;
        }

        public void setCommunityName(String communityName) {
            this.communityName = communityName;
        }

        public String getAppCode() {
            return appCode;
        }

        public void setAppCode(String appCode) {
            this.appCode = appCode;
        }

        public String getModuleCode() {
            return moduleCode;
        }

        public void setModuleCode(String moduleCode) {
            this.moduleCode = moduleCode;
        }

        public static class HiveColumn {
            private String columnName;
            private String dataType;
            private Integer columnOrder;
            private boolean isPrimaryKey;
            private boolean isBusinessKey;
            private boolean isSurrogateKey;
            private String foreignKeyTable;
            private String foreignKeyColumn;

            public HiveColumn(){};

            public HiveColumn(String cn, String dt) {
                this.columnName = cn;
                this.dataType = dt;

            }
            public String getColumnName() {
                return columnName;
            }

            public void setColumnName(String columnName) {
                this.columnName = columnName;
            }

            public String getDataType() {
                return dataType;
            }

            public void setDataType(String dataType) {
                this.dataType = dataType;
            }

            public Integer getColumnOrder() {
                return columnOrder;
            }

            public void setColumnOrder(Integer columnOrder) {
                this.columnOrder = columnOrder;
            }

            public boolean getIsPrimaryKey() {
                return isPrimaryKey;
            }

            public void setIsPrimaryKey(boolean primaryKey) {
                isPrimaryKey = primaryKey;
            }

            public boolean getIsBusinessKey() {
                return isBusinessKey;
            }

            public void setIsBusinessKey(boolean businessKey) {
                isBusinessKey = businessKey;
            }

            public boolean getIsSurrogateKey() {
                return isSurrogateKey;
            }

            public void setIsSurrogateKey(boolean surrogateKey) {
                isSurrogateKey = surrogateKey;
            }

            public String getForeignKeyTable() {
                return foreignKeyTable;
            }

            public void setForeignKeyTable(String foreignKeyTable) {
                this.foreignKeyTable = foreignKeyTable;
            }

            public String getForeignKeyColumn() {
                return foreignKeyColumn;
            }

            public void setForeignKeyColumn(String foreignKeyColumn) {
                this.foreignKeyColumn = foreignKeyColumn;
            }

        }

        public static class OrderByHiveColumnName implements Comparator<HiveColumn> {

            public int compare(HiveColumn o1, HiveColumn o2) {
                return o1.getColumnName().compareTo(o2.getColumnName());
            }
        }

        public static class OrderByHiveColumnOrder implements Comparator<HiveColumn> {

            public int compare(HiveColumn o1, HiveColumn o2) {
                return o1.columnOrder - o2.columnOrder;
            }
        }
    }