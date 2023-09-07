package com.example.demo.model;

public enum SearchableType {
    CUSTOMER("C"),
    POLICY("P"),
    OTHER("O");

    private final String value;

    SearchableType(
            final String searchableTypeName
    ) {
        this.value = searchableTypeName;
    }

    public String getValue() {
        return value;
    }

    public static SearchableType fromString(final String searchableTypeName) {
        if (searchableTypeName != null) {
            for (SearchableType searchableType : SearchableType.values()) {
                if (searchableTypeName.equals(searchableType.value)) {
                    return searchableType;
                }
            }
        }
        return OTHER;
    }

    @Override
    public String toString() {
        return "SearchableType{"
                + "value='" + value + "'"
                + "}";
    }
}
