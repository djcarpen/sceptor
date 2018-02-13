package com.github.djcarpen.sceptor.Schema.DataVault;

import com.github.djcarpen.sceptor.Schema.DataDictionary;
import com.github.djcarpen.sceptor.Schema.HiveTable;
import com.github.djcarpen.sceptor.Utils.RuleFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class HubKey {
    RuleFormatter ruleFormatter = new RuleFormatter();
    private List<HiveTable.HiveColumn> businessKeys;

    public String getHubKey(DataDictionary.Table table) {
        StringJoiner hubKeyJoiner = new StringJoiner(", ");
        businessKeys = new ArrayList<>();
        for (DataDictionary.Table.Column c : table.getColumns()) {
            if (c.getIsBusinessKey()) {
                HiveTable.HiveColumn businessKey = new HiveTable.HiveColumn();
                if (c.getColumnName().equals("id")) {
                    businessKey.setColumnName(table.getTableName() + "_" + c.getColumnName());
                } else {
                    businessKey.setColumnName(c.getColumnName());
                }
                businessKey.setDataType(c.getDataType());
                businessKeys.add(businessKey);
            }
        }
        for (HubSchema.HubTable.HiveColumn c : businessKeys) {
            hubKeyJoiner.add(ruleFormatter.getFormattedColumnDefinition(c.getColumnName(), c.getDataType()));
        }
        return hubKeyJoiner.toString();
    }
}
