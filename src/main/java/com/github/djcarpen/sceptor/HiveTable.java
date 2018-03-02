package com.github.djcarpen.sceptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HiveTable {

    private final List<HiveColumn> partitionColumns = new ArrayList<>();
    private final List<HiveColumn> columns = new ArrayList<>();
    private String databaseName;
    private String hdfsLocation;
    private String tableName;
    private String sourceTableName;
    private HiveTable sourceTable;
    private String storageFormat;
    private boolean isExternalTable;
    private String fieldTerminator;
    private String sourceDatabaseName;


    public String getSourceDatabaseName() {
        return sourceDatabaseName;
    }

    public void setSourceDatabaseName(String sourceDatabaseName) {
        this.sourceDatabaseName = sourceDatabaseName;
    }

    public String getFieldTerminator() {
        return fieldTerminator;
    }

    public void setFieldTerminator(String fieldTerminator) {
        this.fieldTerminator = fieldTerminator;
    }

    public String getStorageFormat() {
        return storageFormat;
    }

    public void setStorageFormat(String storageFormat) {
        this.storageFormat = storageFormat;
    }

    public boolean isExternalTable() {
        return isExternalTable;
    }

    public void setExternalTable(boolean externalTable) {
        isExternalTable = externalTable;
    }

//    public HiveTable getSourceTable() {
//        return sourceTable;
//    }
//
//    public void setSourceTable(HiveTable sourceTable) {
//        this.sourceTable = sourceTable;
//    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getHdfsLocation() {
        return hdfsLocation;
    }

    public void setHdfsLocation(String hdfsLocation) {
        this.hdfsLocation = hdfsLocation;
    }

    public List<HiveColumn> getPartitionColumns() {
        return partitionColumns;
    }

    public void addPartitionColumn(HiveColumn partitionColumn) {
        partitionColumns.add(partitionColumn);
    }

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

    public void addColumn(HiveColumn column) {
        columns.add(column);
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
        private String sourceTableName;
        private String sourceColumnName;

        public String getSourceTableName() {
            return sourceTableName;
        }

        public void setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
        }

        public String getSourceColumnName() {
            return sourceColumnName;
        }

        public void setSourceColumnName(String sourceColumnName) {
            this.sourceColumnName = sourceColumnName;
        }

        public HiveColumn() {
        }

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

        public void setColumnOrder(Integer columnOrder) {
            this.columnOrder = columnOrder;
        }

        public void setForeignKeyTable(String foreignKeyTable) {
            this.foreignKeyTable = foreignKeyTable;
        }

        public void setForeignKeyColumn(String foreignKeyColumn) {
            this.foreignKeyColumn = foreignKeyColumn;
        }

        public String getForeignKeyTable() {
            return foreignKeyTable;
        }
    }

    public static class OrderByHiveColumnName implements Comparator<HiveColumn> {

        public int compare(HiveColumn o1, HiveColumn o2) {

            return o1.getColumnName().compareTo(o2.getColumnName());
        }
    }

}