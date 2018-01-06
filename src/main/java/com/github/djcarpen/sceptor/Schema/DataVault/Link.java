package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Link implements DataVaultSchema {

    private List<HiveTable> linkTables = new ArrayList<>();
    private HiveTable.HiveColumn linkKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> linkColumns;

    public List<HiveTable> getTables() {
        return linkTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (HiveTable t : dataDictionary.getTables()) {
            generateColumns(t);
            if (linkColumns.size() > 0) {
                HiveTable linkTable = new HiveTable();
                linkTable.setDatabaseName(t.getDatabaseName());
                linkTable.setTableName("L_" + t.getTableName());
            for (HiveTable.HiveColumn c : linkColumns) {
                linkTable.addColumn(c);
            }
                linkTables.add(linkTable);
        }
        }

    }

    public void generateColumns(HiveTable sourceTable) {
        linkKey = new HiveTable.HiveColumn("lk_" + sourceTable.getTableName(), "STRING"); // create linkTable key column
        loadDate = new HiveTable.HiveColumn("load_dt", "TIMESTAMP");
        linkColumns = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {

            if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && !c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().replace("_id", "").equals(c.getForeignKeyColumn())) {

                    HiveTable.HiveColumn linkColumn = new HiveTable.HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                    linkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                    linkColumn.setForeignKeyTable("H_"+c.getForeignKeyTable());
                    linkColumns.add(linkColumn);
                } else {
                    linkColumns.add(determineLinkColumnDefinition(c));

                }
            }


        }
        if (linkColumns.size() > 0) linkColumns = sortColumns();
    }

    private HiveTable.HiveColumn determineLinkColumnDefinition(HiveTable.HiveColumn foreignKeyColumn) {
        HiveTable.HiveColumn mappedLinkColumn;

        List<String> foreignKeyTokens = new ArrayList<>();
        StringTokenizer foreignKeyTokenizer = new StringTokenizer(foreignKeyColumn.getColumnName().replace("_id", ""), "_");
        while (foreignKeyTokenizer.hasMoreElements()) {
            foreignKeyTokens.add(foreignKeyTokenizer.nextToken());
        }

        List<String> hubKeyTokens = new ArrayList<>();
        StringTokenizer hubKeyTokenizer = new StringTokenizer((foreignKeyColumn.getForeignKeyTable()), "_");
        while (hubKeyTokenizer.hasMoreElements()) {
            hubKeyTokens.add(hubKeyTokenizer.nextToken());
        }

        List<String> hubKeyTokensSingular = new ArrayList<>();
        StringTokenizer hubKeyTokenizerSingular = new StringTokenizer((foreignKeyColumn.getForeignKeyTable().substring(0, foreignKeyColumn.getForeignKeyTable().length() - 1)), "_");
        while (hubKeyTokenizerSingular.hasMoreElements()) {
            hubKeyTokensSingular.add(hubKeyTokenizerSingular.nextToken());
        }

        if (hubKeyTokens.containsAll(foreignKeyTokens)) {
            mappedLinkColumn = new HiveTable.HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable(),"STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_"+foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setColumnName("hk_" + foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setDataType("STRING");
            //linkColumns.add(mappedLinkColumn);
        } else if (hubKeyTokensSingular.containsAll(foreignKeyTokens)) {
            mappedLinkColumn = new HiveTable.HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable(),"STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_"+foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setColumnName("hk_" + foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setDataType("STRING");
            //linkColumns.add(mappedLinkColumn);
        } else {
            mappedLinkColumn = new HiveTable.HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable() + "_" + foreignKeyColumn.getColumnName().replace("_id", ""),"STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_"+foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setColumnName("hk_" + foreignKeyColumn.getForeignKeyTable() + "_" + foreignKeyColumn.getColumnName().replace("_id", ""));
//            mappedLinkColumn.setDataType("STRING");
            //linkColumns.add(mappedLinkColumn);
        }
        return mappedLinkColumn;
        }

    public List<HiveTable.HiveColumn> sortColumns() {
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
         columnList.add(linkKey);
        linkColumns.sort(new HiveTable.OrderByHiveColumnName());
            columnList.addAll(linkColumns);
            columnList.add(loadDate);
            for (HiveTable.HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
