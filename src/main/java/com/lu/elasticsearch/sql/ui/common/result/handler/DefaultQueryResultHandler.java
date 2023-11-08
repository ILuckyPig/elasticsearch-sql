package com.lu.elasticsearch.sql.ui.common.result.handler;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lu.elasticsearch.sql.ui.common.table.Table;
import com.lu.elasticsearch.sql.ui.common.table.scheme.TableScheme;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;

import java.util.*;
import java.util.stream.Collectors;

import static com.lu.elasticsearch.sql.ui.constant.SqlConstant.EXCLUDE;
import static com.lu.elasticsearch.sql.ui.constant.SqlConstant.SQL_PARSER;

public class DefaultQueryResultHandler implements ResultHandler {
    @Override
    public Table getTable(String sql, String data) throws JsonProcessingException {
        Query query = (Query) SQL_PARSER.createStatement(sql, ParsingOptions.builder().build());
        QuerySpecification queryBody = (QuerySpecification) query.getQueryBody();
        List<SelectItem> selectItems = queryBody.getSelect().getSelectItems();

        String[] columns = getColumns(selectItems);
        boolean refresh = getRefresh(selectItems);

        TableScheme tableScheme = new TableScheme();
        tableScheme.setColumns(columns);
        tableScheme.setRefresh(refresh);
        List<String[]> tableData = getTableData(tableScheme, data);
        return new Table(tableScheme, tableData);
    }

    private String[] getColumns(List<SelectItem> selectItems) {
        return selectItems.stream()
                .map(selectItem -> {
                    if (selectItem instanceof SingleColumn) {
                        SingleColumn item = (SingleColumn) selectItem;
                        if (item.getAlias().isPresent()) {
                            return item.getAlias().get().getValue();
                        }

                        Expression expression = item.getExpression();
                        if (expression instanceof Identifier) {
                            return ((Identifier) expression).getValue();
                        } else if (expression instanceof FunctionCall) {
                            // query with function. e.g. include exclude.
                            FunctionCall functionCall = (FunctionCall) expression;
                            QualifiedName functionCallName = functionCall.getName();
                            // if the func is 'exclude', get the table scheme by _source.
                            if (!EXCLUDE.equals(functionCallName.getParts().get(0))) {
                                List<Expression> arguments = functionCall.getArguments();
                                for (Expression argument : arguments) {
                                    if (argument instanceof StringLiteral) {
                                        return ((StringLiteral) argument).getValue();
                                    }
                                }
                            }
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    private boolean getRefresh(List<SelectItem> selectItems) {
        boolean refresh = false;
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn item = (SingleColumn) selectItem;
                Expression expression = item.getExpression();
                if (expression instanceof FunctionCall) {
                    // query with function. e.g. include exclude.
                    FunctionCall functionCall = (FunctionCall) expression;
                    QualifiedName functionCallName = functionCall.getName();
                    // if the func is 'exclude', get the table scheme by _source.
                    if (EXCLUDE.equals(functionCallName.getParts().get(0))) {
                        refresh = true;
                    }
                }
            } else {
                // query with '*'
                refresh = true;
            }
        }
        return refresh;
    }

    private List<String[]> getTableData(TableScheme tableScheme, String data) throws JsonProcessingException {
        ObjectNode dataNode = JsonUtil.readValue(data, ObjectNode.class);
        JsonNode hits = dataNode.get("hits").get("hits");
        if (hits.isEmpty()) {
            return new ArrayList<>(0);
        }

        if (tableScheme.isRefresh()) {
            updateTableColumns(tableScheme, hits);
        }

        List<String[]> dataList = new ArrayList<>(hits.size());
        for (JsonNode hit : hits) {
            JsonNode source = hit.get("_source");
            JsonNode fields = hit.get("fields");
            String[] row = getRow(tableScheme, source, fields);
            dataList.add(row);
        }
        return dataList;
    }

    private void updateTableColumns(TableScheme tableScheme, JsonNode hits) {
        List<String> columnList = Arrays.stream(tableScheme.getColumns()).collect(Collectors.toList());
        Set<String> existingColumnSet = new HashSet<>(columnList);
        for (JsonNode hit : hits) {
            JsonNode source = hit.get("_source");
            Iterator<String> fieldNameIterator = source.fieldNames();
            while (fieldNameIterator.hasNext()) {
                String fieldName = fieldNameIterator.next();
                if (!existingColumnSet.contains(fieldName)) {
                    existingColumnSet.add(fieldName);
                    columnList.add(fieldName);
                }
            }
        }
        tableScheme.setColumns(columnList.toArray(new String[0]));
    }

    private String[] getRow(TableScheme tableScheme, JsonNode source, JsonNode fields) throws JsonProcessingException {
        String[] columns = tableScheme.getColumns();
        String[] row = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            JsonNode cell = getCellCoalesce(column, source, fields);
            if (cell.isContainerNode()) {
                row[i] = JsonUtil.toJsonString(cell);
            } else {
                row[i] = cell.asText();
            }
        }
        return row;
    }

    private JsonNode getCellCoalesce(String column, JsonNode source, JsonNode fields) {
        JsonNode cell;
        if (source.has(column)) {
            cell = source.get(column);
        } else if (fields == null) {
            cell = NullNode.getInstance();
        } else {
            JsonNode field = fields.get(column);
            if (field.size() == 1) {
                cell = field.get(0);
            } else {
                cell = field;
            }
        }
        return cell;
    }
}
