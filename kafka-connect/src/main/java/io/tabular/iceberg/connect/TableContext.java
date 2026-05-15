package io.tabular.iceberg.connect;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.regex.Pattern;

public class TableContext {
    private final TableIdentifier tableIdentifier;
    private final String branch;

    public static String parseBranch(String tableName, String delimiter) {
        return TableContext.parse(TableIdentifier.parse(tableName), delimiter).branch();
    }

    public static String parseName(String tableName, String delimiter) {
        return TableContext.parse(TableIdentifier.parse(tableName), delimiter).tableIdentifier().toString();
    }

    public static TableContext parse(TableIdentifier tableIdentifier, String delimiter) {
        Namespace namespace = tableIdentifier.namespace();
        String name = tableIdentifier.name();
        String branch = null;

        if (delimiter != null) {
            String[] nameParts = name.split(Pattern.quote(delimiter));
            if (nameParts.length == 2) {
                name = nameParts[0];
                branch = nameParts[1];
            }
        }

        return new TableContext(TableIdentifier.of(namespace, name), branch);
    }

    public TableContext(TableIdentifier tableIdentifier, String branch) {
        this.tableIdentifier = tableIdentifier;
        this.branch = branch;
    }

    public String branch() {
        return branch;
    }

    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }
}