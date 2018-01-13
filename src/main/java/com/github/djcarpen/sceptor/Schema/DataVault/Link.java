package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.DataDictionary.Table.Column;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Schema.HiveTable.HiveColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import static com.github.djcarpen.sceptor.Schema.DataVault.EntityType.LINK;

public class Link implements DataVaultSchema {

    public static final EntityType entityType = LINK;
    private final List<HiveTable> linkTables = new ArrayList<>();
    private HiveColumn linkKey;
    private HiveColumn loadDate;
    private List<HiveColumn> linkColumns;

    public static EntityType getEntityType() {
        return entityType;
    }

    public List<HiveTable> getTables() {
        return linkTables;
    }

    public void generateTables(DataDictionary dataDictionary) {
        for (DataDictionary.Table t : dataDictionary.getTables()) {
            generateColumns(t);
            if (linkColumns.size() > 0) {
                HiveTable linkTable = new HiveTable();
                linkTable.setDatabaseName(t.getDatabaseName());
                linkTable.setTableName("L_" + t.getTableName());
                for (HiveColumn c : linkColumns) {
                    linkTable.addColumn(c);
                }
                linkTables.add(linkTable);
            }
        }

    }

    public void generateColumns(DataDictionary.Table sourceTable) {
        linkKey = new HiveColumn("lk_" + sourceTable.getTableName(), "STRING"); // create linkTable key column
        loadDate = new HiveColumn("load_dt", "TIMESTAMP");
        linkColumns = new ArrayList<>();
        for (Column c : sourceTable.getColumns()) {
            if (!c.getIsBusinessKey() && !c.getIsSurrogateKey() && !c.getForeignKeyColumn().isEmpty()) {
                if (c.getColumnName().replace("_id", "").equals(c.getForeignKeyColumn())) {
                    HiveColumn linkColumn = new HiveColumn("hk_" + c.getForeignKeyTable(), "STRING");
                    linkColumn.setForeignKeyColumn("hk_" + c.getForeignKeyTable());
                    linkColumn.setForeignKeyTable("H_" + c.getForeignKeyTable());
                    linkColumns.add(linkColumn);
                } else {
                    linkColumns.add(determineLinkColumnDefinition(c));
                }
            }
        }
        if (linkColumns.size() > 0) linkColumns = sortColumns();
    }

    private HiveColumn determineLinkColumnDefinition(Column foreignKeyColumn) {
        HiveColumn mappedLinkColumn;
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
            mappedLinkColumn = new HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable(), "STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_" + foreignKeyColumn.getForeignKeyTable());
        } else if (hubKeyTokensSingular.containsAll(foreignKeyTokens)) {
            mappedLinkColumn = new HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable(), "STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_" + foreignKeyColumn.getForeignKeyTable());
        } else {
            mappedLinkColumn = new HiveColumn("hk_" + foreignKeyColumn.getForeignKeyTable() + "_" + foreignKeyColumn.getColumnName().replace("_id", ""), "STRING");
            mappedLinkColumn.setForeignKeyColumn("hk_" + foreignKeyColumn.getForeignKeyTable());
            mappedLinkColumn.setForeignKeyTable("H_" + foreignKeyColumn.getForeignKeyTable());
        }
        return mappedLinkColumn;
    }

    public List<HiveColumn> sortColumns() {
        List<HiveColumn> columnList = new ArrayList<>();
        columnList.add(linkKey);
        linkColumns.sort(new HiveTable.OrderByHiveColumnName());
        columnList.addAll(linkColumns);
        columnList.add(loadDate);
        for (HiveColumn c : columnList) {
            c.setColumnOrder(columnList.indexOf(c));
        }
        return columnList;
    }
}
