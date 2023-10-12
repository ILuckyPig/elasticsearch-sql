package com.lu.elasticsearch.sql.ui.common.result.handler;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lu.elasticsearch.sql.ui.common.table.Table;
import com.lu.elasticsearch.sql.ui.common.table.scheme.TopHitsColumnInfo;
import com.lu.elasticsearch.sql.ui.common.table.scheme.TopHitsTableScheme;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;

import java.util.*;

import static com.lu.elasticsearch.sql.ui.constant.SqlConstant.*;

public class TopHitsQueryResultHandler implements ResultHandler {
    @Override
    public Table getTable(String sql, String data) throws JsonProcessingException {
        Query query = (Query) SQL_PARSER.createStatement(sql, ParsingOptions.builder().build());
        QuerySpecification queryBody = (QuerySpecification) query.getQueryBody();
        List<SelectItem> selectItems = queryBody.getSelect().getSelectItems();
        Optional<GroupBy> groupByOptional = queryBody.getGroupBy();


        TopHitsColumnInfo topHitsColumnInfo = getTopHitsColumn(selectItems);

        String[] groupByColumns = new String[0];
        if (groupByOptional.isPresent()) {
            groupByColumns = getGroupByColumns(groupByOptional.get());
            checkGroupByColumns(groupByColumns, topHitsColumnInfo.getOriginalColumns());
        }

        JsonNode firstHit = getFirstHit(data, topHitsColumnInfo, groupByColumns);
        String[] topHitsColumns = getTopHitsColumns(topHitsColumnInfo, firstHit);
        TopHitsTableScheme topHitsTableScheme = getTopHitsTableScheme(groupByColumns, topHitsColumnInfo, topHitsColumns);

        List<String[]> tableData = getTableData(topHitsTableScheme, data);

        return new Table(topHitsTableScheme, tableData);
    }

    private TopHitsColumnInfo getTopHitsColumn(List<SelectItem> selectItems) {
        List<String> columns = new ArrayList<>();
        TopHitsColumnInfo topHitsColumnInfo = new TopHitsColumnInfo();

        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn item = (SingleColumn) selectItem;
                Expression expression = item.getExpression();
                if (expression instanceof Identifier) {
                    if (item.getAlias().isPresent()) {
                        columns.add(item.getAlias().get().getValue());
                    } else {
                        columns.add(((Identifier) expression).getValue());
                    }
                    continue;
                }

                if (expression instanceof FunctionCall) {
                    FunctionCall functionCall = (FunctionCall) expression;
                    String functionCallName = functionCall.getName().getOriginalParts().get(0);
                    List<Expression> arguments = functionCall.getArguments();

                    if (TOP_HITS.equals(functionCallName)) {
                        parseTopHitFunc(item, functionCallName, arguments, topHitsColumnInfo, columns);
                    } else {
                        parseFunc(item, functionCall, functionCallName, arguments, columns);
                    }
                }
            }
        }
        topHitsColumnInfo.setOriginalColumns(columns.toArray(new String[0]));
        return topHitsColumnInfo;
    }

    private TopHitsTableScheme getTopHitsTableScheme(String[] groupByColumns, TopHitsColumnInfo topHitsColumnInfo,
                                                     String[] topHitsColumns) {
        String[] columns = topHitsColumnInfo.getOriginalColumns();

        int topHitsColumnIndex = 0;
        for (; topHitsColumnIndex < columns.length; topHitsColumnIndex++) {
            if (columns[topHitsColumnIndex].equals(topHitsColumnInfo.getTopHitsName())) {
                break;
            }
        }

        List<String> newColumns = new ArrayList<>();
        newColumns.addAll(Arrays.asList(columns).subList(0, topHitsColumnIndex));
        newColumns.addAll(Arrays.asList(topHitsColumns));
        if (topHitsColumnIndex != columns.length - 1) {
            newColumns.addAll(Arrays.asList(columns).subList(topHitsColumnIndex + 1, columns.length));
        }

        TopHitsTableScheme topHitsTableScheme = new TopHitsTableScheme();
        topHitsTableScheme.setColumns(newColumns.toArray(new String[0]));
        topHitsTableScheme.setGroupByColumns(groupByColumns);
        topHitsTableScheme.setTopHitsName(topHitsColumnInfo.getTopHitsName());
        topHitsTableScheme.setTopHitsColumnStartIndex(topHitsColumnIndex);
        topHitsTableScheme.setTopHitsColumnEndIndex(topHitsColumnIndex + topHitsColumns.length);

        return topHitsTableScheme;
    }

    private void parseTopHitFunc(SingleColumn item, String functionCallName, List<Expression> arguments,
                                 TopHitsColumnInfo topHitsScheme, List<String> columns) {
        String topHitsColumn;
        boolean includeAllColumns = false;
        String[] includeColumns = null;
        List<String> topHitsArguments = new ArrayList<>();
        for (Expression argument : arguments) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) argument;
            String left = ((Identifier) comparisonExpression.getLeft()).getValue();
            Expression right = comparisonExpression.getRight();

            String rightValue = null;
            if (right instanceof LongLiteral) {
                rightValue = String.valueOf(((LongLiteral) right).getValue());
            } else if (right instanceof StringLiteral) {
                rightValue = ((StringLiteral) right).getValue();
            }
            topHitsArguments.add(left + comparisonExpression.getOperator().getValue() + rightValue);

            if (INCLUDE.equals(left) && !ALL_COLUMNS.equals(rightValue)) {
                includeColumns = rightValue.split(",");
            }
            if (INCLUDE.equals(left) && ALL_COLUMNS.equals(rightValue)) {
                includeAllColumns = true;
            }
        }
        if (item.getAlias().isPresent()) {
            topHitsColumn = item.getAlias().get().getValue();
        } else {
            topHitsColumn = functionCallName + "(" + String.join(",", topHitsArguments) + ")";
        }

        columns.add(topHitsColumn);
        topHitsScheme.setTopHitsName(topHitsColumn);
        topHitsScheme.setIncludeAllColumns(includeAllColumns);
        topHitsScheme.setIncludeColumns(includeColumns);
    }

    private void parseFunc(SingleColumn item, FunctionCall functionCall, String functionCallName,
                           List<Expression> arguments, List<String> columns) {
        String column = null;
        if (item.getAlias().isPresent()) {
            column = item.getAlias().get().getValue();
        } else {
            // if query with count(*)
            if (COUNT.equalsIgnoreCase(functionCallName) && arguments.isEmpty()) {
                column = "COUNT(*)";
            } else {
                String distinct = "";
                if (functionCall.isDistinct()) {
                    distinct = "DISTINCT ";
                }
                for (Expression argument : arguments) {
                    if (argument instanceof Identifier) {
                        column = functionCallName.toUpperCase()
                                + "("
                                + distinct
                                + ((Identifier) argument).getValue()
                                + ")";
                    }
                }
            }
        }
        columns.add(column);
    }

    private String[] getGroupByColumns(GroupBy groupBy) {
        return groupBy.getGroupingElements()
                .stream()
                .flatMap(groupingElement -> groupingElement.getExpressions().stream())
                .map(expression -> ((Identifier) expression).getValue())
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    private JsonNode getFirstHit(String data, TopHitsColumnInfo topHitsColumnInfo, String[] groupByColumns) throws JsonProcessingException {
        ObjectNode dataNode = JsonUtil.readValue(data, ObjectNode.class);
        JsonNode aggregations = dataNode.get("aggregations");
        for (String groupByColumn : groupByColumns) {
            aggregations = aggregations
                    .get(groupByColumn)
                    .get("buckets")
                    .get(0);
        }
        return aggregations.get(topHitsColumnInfo.getTopHitsName())
                .get("hits")
                .get("hits")
                .get(0)
                .get("_source");
    }

    private String[] getTopHitsColumns(TopHitsColumnInfo topHitsColumnInfo, JsonNode firstHit) {
        if (topHitsColumnInfo.isIncludeAllColumns()) {
            List<String> list = new ArrayList<>();
            firstHit.fieldNames().forEachRemaining(list::add);
            return list.toArray(new String[0]);
        } else {
            return Arrays.stream(topHitsColumnInfo.getIncludeColumns())
                    .filter(firstHit::has)
                    .toArray(String[]::new);
        }
    }

    private List<String[]> getTableData(TopHitsTableScheme topHitsTableScheme, String data) throws JsonProcessingException {
        ObjectNode dataNode = JsonUtil.readValue(data, ObjectNode.class);
        JsonNode aggregations = dataNode.get("aggregations");
        List<String[]> tableData = new ArrayList<>();
        String[] groupByValues = new String[topHitsTableScheme.getGroupByColumns().length];
        parseAggregations(aggregations, tableData, groupByValues, 0, topHitsTableScheme);
        return tableData;
    }

    public void parseAggregations(JsonNode bucket, List<String[]> tableData, String[] groupByValues, int deepNum,
                                  TopHitsTableScheme topHitsTableScheme) throws JsonProcessingException {
        String[] groupByColumns = topHitsTableScheme.getGroupByColumns();
        if (deepNum == groupByColumns.length) {
            List<String[]> rows = getRow(groupByValues, topHitsTableScheme, bucket);
            tableData.addAll(rows);
            return;
        }
        String groupByColumn = groupByColumns[deepNum];
        JsonNode buckets = bucket.get(groupByColumn).get("buckets");
        int groupByColumnIndex = deepNum;
        deepNum++;
        for (JsonNode subBucket : buckets) {
            String keyValue = subBucket.get("key").asText();
            groupByValues[groupByColumnIndex] = keyValue;
            parseAggregations(subBucket, tableData, groupByValues, deepNum, topHitsTableScheme);
        }
    }

    private List<String[]> getRow(String[] groupByValues, TopHitsTableScheme topHitsTableScheme, JsonNode bucket) throws JsonProcessingException {
        List<String[]> rows = new ArrayList<>();
        JsonNode hits = bucket.get(topHitsTableScheme.getTopHitsName())
                .get("hits")
                .get("hits");
        for (JsonNode hit : hits) {
            JsonNode source = hit.get("_source");
            String[] columns = topHitsTableScheme.getColumns();
            String[] row = new String[columns.length];

            int start = topHitsTableScheme.getGroupByColumns().length;
            for (int i = start; i < columns.length; i++) {
                String column = columns[i];
                JsonNode cell;
                if (topHitsTableScheme.getTopHitsColumnStartIndex() <= i
                        && i < topHitsTableScheme.getTopHitsColumnEndIndex()) {
                    cell = source.get(column);
                } else {
                    cell = bucket.get(column).get("value");
                }

                if (cell.isContainerNode()) {
                    row[i] = JsonUtil.toJsonString(cell);
                } else {
                    row[i] = cell.asText();
                }
            }

            System.arraycopy(groupByValues, 0, row, 0, groupByValues.length);
            rows.add(row);
        }

        return rows;
    }

    private void checkGroupByColumns(String[] groupByColumns, String[] columns) {
        for (int i = 0; i < groupByColumns.length; i++) {
            String column = columns[i];
            String groupByColumn = groupByColumns[i];
            if (!column.equals(groupByColumn)) {
                throw new RuntimeException("the query column must mapping to the grouping column");
            }
        }
    }
}
