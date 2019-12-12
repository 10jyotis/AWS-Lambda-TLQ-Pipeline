package saaf;

import java.sql.Date;

/**
 * A basic Response object that can be consumed by FaaS Inspector
 * to be used as additional output.
 *
 * @author Jyoti Shankar
 */
public class Response {

    private String region;
    private String country;
    private String itemType;
    private String salesChannel;
    private String orderPriority;
    private Date orderDate;
    private Integer orderId;
    private Date shipDate;
    private Integer unitsSold;
    private Float unitPrice;
    private Float unitCost;
    private Float totalRevenue;
    private Float totalCost;
    private Float totalProfit;
    private Integer orderProcessingTime;
    private Float grossMargin;

    public String getRegion() {
        return region;
    }

    public String getCountry() {
        return country;
    }

    public String getItemType() {
        return itemType;
    }

    public String getSalesChannel() {
        return salesChannel;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public Date getShipDate() {
        return shipDate;
    }

    public Integer getUnitsSold() {
        return unitsSold;
    }

    public Float getUnitPrice() {
        return unitPrice;
    }

    public Float getUnitCost() {
        return unitCost;
    }

    public Float getTotalRevenue() {
        return totalRevenue;
    }

    public Float getTotalCost() {
        return totalCost;
    }

    public Float getTotalProfit() {
        return totalProfit;
    }

    public Integer getOrderProcessingTime() {
        return orderProcessingTime;
    }

    public Float getGrossMargin() {
        return grossMargin;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    public void setOrderPriority(String orderPriority) {
        this.orderPriority = orderPriority;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public void setShipDate(Date shipDate) {
        this.shipDate = shipDate;
    }

    public void setUnitsSold(Integer unitsSold) {
        this.unitsSold = unitsSold;
    }

    public void setUnitPrice(Float unitPrice) {
        this.unitPrice = unitPrice;
    }

    public void setUnitCost(Float unitCost) {
        this.unitCost = unitCost;
    }

    public void setTotalRevenue(Float totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public void setTotalCost(Float totalCost) {
        this.totalCost = totalCost;
    }

    public void setTotalProfit(Float totalProfit) {
        this.totalProfit = totalProfit;
    }

    public void setOrderProcessingTime(Integer orderProcessingTime) {
        this.orderProcessingTime = orderProcessingTime;
    }

    public void setGrossMargin(Float grossMargin) {
        this.grossMargin = grossMargin;
    }

    // Return value
    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "value=" + this.getValue() + super.toString();
    }
}
