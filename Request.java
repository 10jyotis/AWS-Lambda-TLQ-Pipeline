package lambda;

import java.util.List;

/**
 * @author Jyoti Shankar
 */
public class Request {

    // All these filters get ANDed when provided
    private String region;
    private String country;
    private String itemType;
    private String salesChannel;
    private String orderPriority;

    // Query Aggregation
    private String type;
    private String aggregateByFields;
    private List<String> groupByFields;

    public Request(String region, String country, String itemType, String salesChannel, String orderPriority,
                   String type, String aggregateByFields, List<String> groupByFields) {

        this.region = region;
        this.country = country;
        this.itemType = itemType;
        this.salesChannel = salesChannel;
        this.orderPriority = orderPriority;
        this.type = type;
        this.aggregateByFields = aggregateByFields;
        this.groupByFields = groupByFields;
    }

    public Request() {

    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getSalesChannel() {
        return salesChannel;
    }

    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public void setOrderPriority(String orderPriority) {
        this.orderPriority = orderPriority;
    }

    public String getType() {
        return type;
    }

    public String getAggregateByFields() {
        return aggregateByFields;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setAggregateByFields(String aggregateByFields) {
        this.aggregateByFields = aggregateByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

}
