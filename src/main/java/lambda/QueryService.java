package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.CollectionUtils;
import saaf.Inspector;
import saaf.Response;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Jyoti Shankar
 */
public class QueryService implements RequestHandler<Request, Collection<HashMap<String, Object>>> {

    private static final String WHERE_CLAUSE_FORMAT = "%s = '%s'";
    private static final String SQL_TEMPLATE = "SELECT %s FROM myquerytable WHERE "
            + "%s AND "
            + "%s AND "
            + "%s AND "
            + "%s AND "
            + "%s "
            + "%s";
    private static final String AGGREGATE_CLAUSE_WITH_GROUP_BY_FORMAT = "%s, %s(%s) AS %s";
    private static final String AGGREGATE_CLAUSE_FORMAT = "%s(%s) AS %s";
    private static final String GROUP_BY_CLAUSE_FORMAT = "GROUP BY %s;";
    private static final String SEMI_COLON = ";";
    private static final String STAR = "*";

    private static final Set<String> VALID_AGGREGATE_FUNCTIONS = new HashSet<>(Arrays.asList("AVG", "SUM", "MIN",
            "MAX", "COUNT"));
    private static final Set<String> VALID_COLUMN_NAMES = new HashSet<>(Arrays.asList("region",
            "country",
            "item_type",
            "sales_channel",
            "order_priority",
            "order_date",
            "order_id",
            "ship_date",
            "units_sold",
            "unit_price",
            "unit_cost",
            "total_revenue",
            "total_cost",
            "total_profit",
            "order_processing_time",
            "gross_margin"));


    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return Collection of HashMap that Lambda will automatically convert into JSON.
     */
    public Collection<HashMap<String, Object>> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();
        validateRequest(request);

        Inspector inspector;
        Response response = new Response();
        LinkedList<HashMap<String, Object>> list = new LinkedList<>();

        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");

            Connection con = DriverManager.getConnection(url, username, password);

            PreparedStatement ps = con.prepareStatement(getSqlQuery(request));
            ResultSet rs = ps.executeQuery();

            logger.log("SQL query being executed : " + getSqlQuery(request));

            while (rs.next()) {
                inspector = new Inspector();
                // add logs

                if (isEmptyOrNull(request.getType())) {
                    setLambdaResponse(response, rs, VALID_COLUMN_NAMES);
                } else {
                    List<String> fields = new ArrayList<>();
                    if (request.getGroupByFields() != null) {
                        fields.addAll(request.getGroupByFields());
                    }
                    fields.add(request.getAggregateByFields());
                    setLambdaResponse(response, rs, new HashSet<>(fields));
                }

                inspector.consumeResponse(response);
                list.add(inspector.finish());
            }
            rs.close();
            con.close();

        } catch (SQLException ex) {
            logger.log("Got an exception working with MySQL! \n");
            logger.log(ex.getMessage());
            ex.printStackTrace();
        } catch (IOException ex) {
            logger.log("IO Exception occurred while reading the db.properties file");
        }

        return list;
    }

    private void setLambdaResponse(Response response, ResultSet rs, Set<String> fields) throws SQLException {
        for (String field : fields) {
            switch (field) {
                case "region":
                    response.setRegion(rs.getString("region"));
                    break;
                case "country":
                    response.setCountry(rs.getString("country"));
                    break;
                case "item_type":
                    response.setItemType(rs.getString("item_type"));
                    break;
                case "sales_channel":
                    response.setSalesChannel(rs.getString("sales_channel"));
                    break;
                case "order_priority":
                    response.setOrderPriority(rs.getString("order_priority"));
                    break;
                case "order_date":
                    response.setOrderDate(rs.getDate("order_date"));
                    break;
                case "order_id":
                    response.setOrderId(rs.getInt("order_id"));
                    break;
                case "ship_date":
                    response.setShipDate(rs.getDate("ship_date"));
                    break;
                case "units_sold":
                    response.setUnitsSold(rs.getInt("units_sold"));
                    break;
                case "unit_price":
                    response.setUnitPrice(rs.getFloat("unit_price"));
                    break;
                case "unit_cost":
                    response.setUnitCost(rs.getFloat("unit_cost"));
                    break;
                case "total_revenue":
                    response.setTotalRevenue(rs.getFloat("total_revenue"));
                    break;
                case "total_cost":
                    response.setTotalCost(rs.getFloat("total_cost"));
                    break;
                case "total_profit":
                    response.setTotalProfit(rs.getFloat("total_profit"));
                    break;
                case "order_processing_time":
                    response.setOrderProcessingTime(rs.getInt("order_processing_time"));
                    break;
                case "gross_margin":
                    response.setGrossMargin(rs.getFloat("gross_margin"));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field name.." + field);
            }
        }
    }

    private String getSqlQuery(Request request) {
        return String.format(SQL_TEMPLATE, getSelectClause(request),
                getWhereClause(request.getRegion(), "region"),
                getWhereClause(request.getCountry(), "country"),
                getWhereClause(request.getItemType(), "item_type"),
                getWhereClause(request.getSalesChannel(), "sales_channel"),
                getWhereClause(request.getOrderPriority(), "order_priority"),
                getGroupByClause(request.getGroupByFields()));
    }

    private String getSelectClause(Request request) {
        if (isEmptyOrNull(request.getType())) {
            return STAR;
        } else {
            validateAggregateFunction(request.getType());
            return getAggregateClause(request);
        }
    }

    private String getWhereClause(String filter, String filterName) {
        return (!isEmptyOrNull(filter)) ? String.format(WHERE_CLAUSE_FORMAT, filterName, filter)
                : String.format(WHERE_CLAUSE_FORMAT, 1, 1);
    }

    private String getGroupByClause(List<String> groupByFields) {
        return CollectionUtils.isNullOrEmpty(groupByFields) ? SEMI_COLON
                : String.format(GROUP_BY_CLAUSE_FORMAT, getCommaSeparatedGroupByFields(groupByFields));
    }

    private String getAggregateClause(Request request) {
        if (CollectionUtils.isNullOrEmpty(request.getGroupByFields())) {
            return String.format(AGGREGATE_CLAUSE_FORMAT, request.getType(), request.getAggregateByFields(),
                    request.getAggregateByFields());
        } else {
            return String.format(AGGREGATE_CLAUSE_WITH_GROUP_BY_FORMAT,
                    getCommaSeparatedGroupByFields(request.getGroupByFields()),
                    request.getType(),
                    request.getAggregateByFields(),
                    request.getAggregateByFields());
        }
    }

    private String getCommaSeparatedGroupByFields(List<String> groupByFields) {
        return groupByFields.stream()
                .filter(field -> !isEmptyOrNull(field))
                .collect(Collectors.joining(", "));
    }

    private void validateAggregateFunction(String aggregateFunction) {
        if (!VALID_AGGREGATE_FUNCTIONS.contains(aggregateFunction)) {
            throw new IllegalArgumentException("Aggregate function type specified is invalid");
        }
    }

    private boolean isEmptyOrNull(String str) {
        return str == null || str.isEmpty();
    }

    private void validateRequest(Request request) {
        if (!isEmptyOrNull(request.getType())) {
            checkState(!isEmptyOrNull(request.getAggregateByFields()), "AggregateByField cannot be empty or null when type specified");
            checkState(isValidGroupByField(request.getGroupByFields()), "GroupByFields cannot be null or empty");
        } else {
            checkState(isEmptyOrNull(request.getAggregateByFields()), "AggregateByField is not allowed without an aggregate function");
            checkState(CollectionUtils.isNullOrEmpty(request.getGroupByFields()), "GroupByFields is not allowed without an aggregate function");
        }
    }

    private void checkState(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private boolean isValidGroupByField(List<String> groupByFields) {
        return groupByFields == null || groupByFields.stream().noneMatch(this::isEmptyOrNull);
    }

}
