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

    public String getHubKey(HubSchema.HubTable table, String alias) {
        StringJoiner hubKeyJoiner = new StringJoiner(", ");
        businessKeys = new ArrayList<>();
        for (DataDictionary.Table.Column c : table.getSourceTable().getColumns()) {
            if (c.getIsBusinessKey()) {
                HiveTable.HiveColumn businessKey = new HiveTable.HiveColumn();
                businessKey.setColumnName(c.getColumnName());
                businessKey.setDataType(c.getDataType());
                businessKeys.add(businessKey);
            }
        }
        for (HubSchema.HubTable.HiveColumn c : businessKeys) {
            hubKeyJoiner.add(ruleFormatter.getFormattedColumnDefinition(alias + "." + c.getColumnName(), c.getDataType()));
        }
        return hubKeyJoiner.toString();
    }

}
