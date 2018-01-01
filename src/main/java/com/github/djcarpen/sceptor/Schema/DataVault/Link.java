package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.SourceSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class Link {
    private HiveTable link;
    private List<HiveTable> links = new ArrayList<>();
    private HiveTable.HiveColumn linkKey;
    private HiveTable.HiveColumn loadDate;
    private List<HiveTable.HiveColumn> linkColumns;

    public List<HiveTable> getLinks() {
        return links;
    }

    public void generateLinks(SourceSchema sourceSchema){
        for (HiveTable t : sourceSchema.getTables()) {
            generateLinkColumns(t);
            if (linkColumns.size() > 0) {
            link = new HiveTable();
            link.setDatabaseName(t.getDatabaseName());
            link.setTableName("L_"+t.getTableName());
            for (HiveTable.HiveColumn c : linkColumns) {
                link.addColumn(c);
            }
            links.add(link);
        }
        }

    }

    public void generateLinkColumns(HiveTable sourceTable) {
        linkKey = new HiveTable.HiveColumn("lk_" + sourceTable.getTableName(), "STRING"); // create link key column
        loadDate = new HiveTable.HiveColumn("load_dt", "TIMESTAMP");
        linkColumns = new ArrayList<>();
        for (HiveTable.HiveColumn c : sourceTable.getColumns()) {

            if (c.getIsBusinessKey() == false && c.getIsSurrogateKey() == false && !c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().replace("_id", "").equals(c.getForeignKeyColumn())) {

                    HiveTable.HiveColumn linkColumn = new HiveTable.HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                    linkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                    linkColumn.setForeignKeyTable("H_"+c.getForeignKeyTable());
                    linkColumns.add(linkColumn);
                } else {
                    determineLinkColumnDefinition(c);

                }
            }


        }
        if (linkColumns.size() > 0) linkColumns = sortLinkColumns(linkKey, linkColumns, loadDate);
    }

    public HiveTable.HiveColumn determineLinkColumnDefinition(HiveTable.HiveColumn foreignKeyColumn) {
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
            linkColumns.add(mappedLinkColumn);
        } else if (hubKeyTokensSingular.containsAll(foreignKeyTokens)) {
            mappedLinkColumn = new HiveTable.HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable(),"STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_"+foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setColumnName("hk_" + foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setDataType("STRING");
            linkColumns.add(mappedLinkColumn);
        } else {
            mappedLinkColumn = new HiveTable.HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable() + "_" + foreignKeyColumn.getColumnName().replace("_id", ""),"STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_"+foreignKeyColumn.getForeignKeyTable());
//            mappedLinkColumn.setColumnName("hk_" + foreignKeyColumn.getForeignKeyTable() + "_" + foreignKeyColumn.getColumnName().replace("_id", ""));
//            mappedLinkColumn.setDataType("STRING");
            linkColumns.add(mappedLinkColumn);
        }
        return mappedLinkColumn;
        }

    public List<HiveTable.HiveColumn> sortLinkColumns (HiveTable.HiveColumn linkKey, List<HiveTable.HiveColumn> linkColumns, HiveTable.HiveColumn loadDate) {
        this.linkKey = linkKey;
        this.linkColumns = linkColumns;
        this.loadDate = loadDate;
        List<HiveTable.HiveColumn> columnList = new ArrayList<>();
         columnList.add(linkKey);
            Collections.sort(linkColumns, new HiveTable.OrderByHiveColumnName());
            columnList.addAll(linkColumns);
            columnList.add(loadDate);
            for (HiveTable.HiveColumn c : columnList) {
                c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
