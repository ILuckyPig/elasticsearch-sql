package com.lu.elasticsearch.sql.ui.common.result.handler;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lu.elasticsearch.sql.ui.common.table.Table;
import com.lu.elasticsearch.sql.ui.common.table.scheme.TableScheme;
import com.lu.elasticsearch.sql.ui.constant.SqlConstant;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;

import java.util.*;

public class AggregationQueryResultHandler implements ResultHandler {

    public Table getTable(String sql, String data) throws JsonProcessingException {
        Query query = (Query) SqlConstant.SQL_PARSER.createStatement(sql, ParsingOptions.builder().build());
        QuerySpecification queryBody = (QuerySpecification) query.getQueryBody();
        List<SelectItem> selectItems = queryBody.getSelect().getSelectItems();
        Optional<GroupBy> groupByOptional = queryBody.getGroupBy();
        String[] columns = this.getColumns(selectItems);
        String[] groupByColumns = new String[0];
        if (groupByOptional.isPresent()) {
            groupByColumns = this.getGroupByColumns(groupByOptional.get());

            for (int i = 0; i < groupByColumns.length; ++i) {
                String column = columns[i];
                String groupByColumn = groupByColumns[i];
                if (!column.equals(groupByColumn)) {
                    throw new RuntimeException("the query column must mapping to the grouping column");
                }
            }
        }

        TableScheme tableScheme = new TableScheme();
        tableScheme.setColumns(columns);
        tableScheme.setRefresh(false);
        tableScheme.setGroupByColumns(groupByColumns);
        List<String[]> tableData = this.getTableData(tableScheme, data);
        return new Table(tableScheme, tableData);
    }

    private String[] getColumns(List<SelectItem> selectItems) {
        return selectItems.stream()
                .map((selectItem) -> {
                    if (selectItem instanceof SingleColumn) {
                        SingleColumn item = (SingleColumn) selectItem;
                        if (item.getAlias().isPresent()) {
                            return item.getAlias().get().getValue();
                        }

                        Expression expression = item.getExpression();
                        if (expression instanceof Identifier) {
                            return ((Identifier) expression).getValue();
                        }

                        if (expression instanceof FunctionCall) {
                            FunctionCall functionCall = (FunctionCall) expression;
                            String functionCallName = functionCall.getName().getOriginalParts().get(0);
                            if ("count".equalsIgnoreCase(functionCallName) && functionCall.getArguments().isEmpty()) {
                                return "COUNT(*)";
                            }

                            String distinct = "";
                            if (functionCall.isDistinct()) {
                                distinct = "DISTINCT ";
                            }

                            List<Expression> arguments = functionCall.getArguments();
                            for (Expression argument : arguments) {
                                if (argument instanceof Identifier) {
                                    return functionCallName.toUpperCase() + "(" + distinct + ((Identifier) argument).getValue() + ")";
                                }
                            }
                        }
                    }

                    return null;
                }).filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    private String[] getGroupByColumns(GroupBy groupBy) {
        return groupBy.getGroupingElements().stream()
                .flatMap((groupingElement) -> groupingElement.getExpressions().stream())
                .map((expression) -> ((Identifier) expression).getValue())
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    private List<String[]> getTableData(TableScheme tableScheme, String data) throws JsonProcessingException {
        ObjectNode dataNode = JsonUtil.readValue(data, ObjectNode.class);
        JsonNode aggregations = dataNode.get("aggregations");
        List<String[]> tableData = new ArrayList<>();
        String[] groupByColumns = tableScheme.getGroupByColumns();
        String[] groupByValues = new String[groupByColumns.length];
        String[] columns = tableScheme.getColumns();
        this.parseAggregations(aggregations, tableData, columns, groupByColumns, groupByValues, 0);
        return tableData;
    }

    private void parseAggregations(JsonNode bucket, List<String[]> tableData, String[] columns, String[] groupByColumns, String[] groupByValues, int deepNum) throws JsonProcessingException {
        if (deepNum == groupByColumns.length) {
            String[] row = this.getRow(columns, groupByColumns, bucket);
            System.arraycopy(groupByValues, 0, row, 0, groupByValues.length);
            tableData.add(row);
        } else {
            String groupByColumn = groupByColumns[deepNum];
            JsonNode buckets = bucket.get(groupByColumn).get("buckets");
            int groupByColumnIndex = deepNum++;

            for (JsonNode subBucket : buckets) {
                String keyValue = subBucket.get("key").asText();
                groupByValues[groupByColumnIndex] = keyValue;
                this.parseAggregations(subBucket, tableData, columns, groupByColumns, groupByValues, deepNum);
            }
        }
    }

    private String[] getRow(String[] columns, String[] groupByColumns, JsonNode bucket) throws JsonProcessingException {
        String[] row = new String[columns.length];
        int start = 0;
        if (groupByColumns != null) {
            start = groupByColumns.length;
        }

        for (int i = start; i < columns.length; ++i) {
            String column = columns[i];
            JsonNode cell = bucket.get(column).get("value");
            if (cell.isContainerNode()) {
                row[i] = JsonUtil.toJsonString(cell);
            } else {
                row[i] = cell.asText();
            }
        }

        return row;
    }
}
