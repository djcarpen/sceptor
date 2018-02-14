package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
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
            linkTable.setDatabaseName("rdw_" + t.getDatabaseName());
            linkTable.setTableName("L_" + t.getTableName());

            linkTables.add(linkTable);

        }

    }


    public static class LinkTable extends HiveTable {

        private HiveColumn linkKey;
        private HiveColumn loadDate;
        private List<HiveColumn> linkColumns;
        

        @Override
        public List<HiveColumn> getColumns() {

            return linkColumns;
        }

        public void setColumns(DataDictionary.Table sourceTable) {
            linkColumns = new ArrayList<>();
            for (Column c : sourceTable.getColumns()) {
                if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && !c.getForeignKeyColumn().isEmpty()) {
                    if (c.getColumnName().replace("_id", "").equals(c.getForeignKeyColumn())) {
                        HiveColumn linkColumn = new HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                        linkColumn.setSourceColumnName(c.getColumnName());
                        linkColumn.setSourceTableName(sourceTable.getTableName());

                        linkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                        linkColumn.setForeignKeyTable("H_" + c.getForeignKeyTable());
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

                        if (hubKeyTokens.containsAll(foreignKeyTokens)) {
                            mappedLinkColumn = new HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                            mappedLinkColumn.setSourceColumnName(c.getColumnName());
                            mappedLinkColumn.setSourceTableName(sourceTable.getTableName());
                            mappedLinkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                            mappedLinkColumn.setForeignKeyTable("H_" + c.getForeignKeyTable());
                        } else if (hubKeyTokensSingular.containsAll(foreignKeyTokens)) {
                            mappedLinkColumn = new HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                            mappedLinkColumn.setSourceColumnName(c.getColumnName());
                            mappedLinkColumn.setSourceTableName(sourceTable.getTableName());
                            mappedLinkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                            mappedLinkColumn.setForeignKeyTable("H_" + c.getForeignKeyTable());
                        } else {
                            mappedLinkColumn = new HiveColumn("hk_" + c.getForeignKeyTable() + "_" + c.getColumnName().replace("_id", ""), "STRING");
                            mappedLinkColumn.setSourceColumnName(c.getColumnName());
                            mappedLinkColumn.setSourceTableName(sourceTable.getTableName());
                            mappedLinkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                            mappedLinkColumn.setForeignKeyTable("H_" + c.getForeignKeyTable());
                        }
                        linkColumns.add(mappedLinkColumn);
                    }
                }
            }
            List<HiveColumn> columnList = new ArrayList<>();
            columnList.add(linkKey);
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
            linkKey = new HiveColumn("lk_" + sourceTable.getTableName(), "STRING"); // create hubTable key column
        }

        public HiveColumn getLoadDate() {
            return loadDate;
        }

        public void setLoadDate() {
            this.loadDate = loadDate;
            loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        }


    }
}


