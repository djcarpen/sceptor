package com.github.djcarpen.sceptor.Utils;

public class RuleFormatter {

    public String getFormattedColumnDefinition(String columnName, String dataType) {
        String formattedColumnDefinition = "";
        if (dataType.equals("STRING")) {
            formattedColumnDefinition = "upper(nvl(regexp_replace(" + columnName + ",'\"',''),''))";
        } else {
            formattedColumnDefinition = "nvl(cast(" + columnName + " as STRING),'')";
        }
        return formattedColumnDefinition;
    }

}
