package com.github.djcarpen.sceptor.Schema.RDW;

import com.github.djcarpen.sceptor.DataDictionary;
import com.github.djcarpen.sceptor.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.HiveTable;
import com.github.djcarpen.sceptor.PropertyHandler;
import com.github.djcarpen.sceptor.Schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class LinkSchema implements Schema {

    private final List<LinkTable> linkTables = new ArrayList<>();


    public List<LinkTable> getTables() {
        return linkTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            LinkTable linkTable = new LinkTable();
            linkTable.setSourceTableName(t.getTableName());
            linkTable.setLinkKey(t);
            linkTable.setLoadDate();
            linkTable.setColumns(t);
            linkTable.setDatabaseName(PropertyHandler.getInstance().getValue("rdwDatabaseNamePrefix") + t.getDatabaseName());
            linkTable.setSourceDatabaseName(PropertyHandler.getInstance().getValue("stagingDatabaseNamePrefix") + t.getCommunityName() + "_" + t.getAppCode() + "_" + t.getModuleCode() + "_" + t.getDatabaseName());
            linkTable.setSourceTable(t);
            linkTable.setTableName(PropertyHandler.getInstance().getValue("rdwLinkTablePrefix") + t.getTableName());
            linkTable.setStorageFormat(PropertyHandler.getInstance().getValue("rdwStoredAs"));
            linkTable.setHdfsLocation(PropertyHandler.getInstance().getValue("rdwHdfsBasePath") + "/rdw/" + t.getCommunityName() + "/public/" + t.getDatabaseName() + "/" + PropertyHandler.getInstance().getValue("rdwLinkTablePrefix") + t.getTableName());
            linkTable.addPartitionColumn(linkTable.getLoadDate());
            if (linkTable.getColumns().size() > 3) {
                linkTables.add(linkTable);
            }


        }

    }


    public static class LinkTable extends HiveTable {

        private HiveColumn linkKey;
        private HiveColumn loadDate;
        private List<HiveColumn> linkColumns;
        private DataDictionary.Table sourceTable;

        public DataDictionary.Table getSourceTable() {
            return sourceTable;
        }

        public void setSourceTable(DataDictionary.Table sourceTable) {
            this.sourceTable = sourceTable;
        }

        @Override
        public List<HiveColumn> getColumns() {

            return linkColumns;
        }

        public void setColumns(DataDictionary.Table sourceTable) {
            linkColumns = new ArrayList<>();
            HiveColumn parentTableHubKey = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + sourceTable.getTableName(), "STRING");
            for (Column c : sourceTable.getColumns()) {
                if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && !c.getForeignKeyColumn().isEmpty()) {
                    if (c.getColumnName().replace("_id", "").equals(c.getForeignKeyColumn())) {
                        HiveColumn linkColumn = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable(), "STRING");
                        linkColumn.setSourceColumnName(c.getColumnName());
                        linkColumn.setSourceTableName(sourceTable.getTableName());

                        linkColumn.setForeignKeyColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable());
                        linkColumn.setForeignKeyTable(PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + c.getForeignKeyTable());
                        linkColumns.add(linkColumn);
                    } else {

                        HiveColumn mappedLinkColumn;
                        List<String> foreignKeyTokens = new ArrayList<>();
                        StringTokenizer foreignKeyTokenizer = new StringTokenizer(c.getColumnName().replace("_id", ""), "_");
                        while (foreignKeyTokenizer.hasMoreElements()) {
                            foreignKeyTokens.add(foreignKeyTokenizer.nextToken());
                        }

                        List<String> hubKeyTokens = new ArrayList<>();
                        StringTokenizer hubKeyTokenizer = new StringTokenizer((c.getForeignKeyTable()), "_");
                        while (hubKeyTokenizer.hasMoreElements()) {
                            hubKeyTokens.add(hubKeyTokenizer.nextToken());
                        }
                        List<String> hubKeyTokensSingular = new ArrayList<>();
                        StringTokenizer hubKeyTokenizerSingular = new StringTokenizer((c.getForeignKeyTable().substring(0, c.getForeignKeyTable().length() - 1)), "_");
                        while (hubKeyTokenizerSingular.hasMoreElements()) {
                            hubKeyTokensSingular.add(hubKeyTokenizerSingular.nextToken());
                        }

                        if (hubKeyTokens.containsAll(foreignKeyTokens) || hubKeyTokensSingular.containsAll(foreignKeyTokens)) {
                            mappedLinkColumn = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable(), "STRING");
                            mappedLinkColumn.setSourceColumnName(c.getColumnName());
                            mappedLinkColumn.setSourceTableName(sourceTable.getTableName());
                            mappedLinkColumn.setForeignKeyColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable());
                            mappedLinkColumn.setForeignKeyTable(PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + c.getForeignKeyTable());
                        } else {
                            mappedLinkColumn = new HiveColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable() + "_" + c.getColumnName().replace("_id", ""), "STRING");
                            mappedLinkColumn.setSourceColumnName(c.getColumnName());
                            mappedLinkColumn.setSourceTableName(sourceTable.getTableName());
                            mappedLinkColumn.setForeignKeyColumn(PropertyHandler.getInstance().getValue("rdwHubKeyPrefix") + c.getForeignKeyTable());
                            mappedLinkColumn.setForeignKeyTable(PropertyHandler.getInstance().getValue("rdwHubTablePrefix") + c.getForeignKeyTable());
                        }
                        linkColumns.add(mappedLinkColumn);
                    }
                }
            }
            List<HiveColumn> columnList = new ArrayList<>();
            columnList.add(linkKey);
            columnList.add(parentTableHubKey);
            linkColumns.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(linkColumns);
            columnList.add(loadDate);
            for (HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
            }
            linkColumns = columnList;
        }

        public HiveColumn getLinkKey() {
            return linkKey;
        }

        public void setLinkKey(DataDictionary.Table sourceTable) {
            this.linkKey = linkKey;
            linkKey = new HiveColumn(PropertyHandler.getInstance().getValue("rdwLinkKeyPrefix") + sourceTable.getTableName(), "STRING"); // create hubTable key column
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            loadDate = new HiveColumn(PropertyHandler.getInstance().getValue("rdwLoadDateColumn"), "STRING");
        }


    }
}


