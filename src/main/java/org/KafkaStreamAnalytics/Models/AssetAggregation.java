package org.KafkaStreamAnalytics.Models;

public class AssetAggregation {
    private Long openTimestamp;
    private Double openPrice;
    private Double lowPrice;
    private Double highPrice;
    private Double closePrice;
    private Long closeTimestamp;
    private String isin;


    public AssetAggregation() {
        openTimestamp = 0L;
        openPrice = 0.0;
        lowPrice = 0.0;
        highPrice = 0.0;
        closePrice = 0.0;
        closeTimestamp = 0L;
        isin = "";
    }

    public AssetAggregation addValue(Quote quote, String isin) {

        if (openTimestamp == 0) {
            this.isin = isin;
            openPrice = quote.getPrice();
            lowPrice = quote.getPrice();
            highPrice = quote.getPrice();
            closePrice = quote.getPrice();
            closeTimestamp = quote.getTimestamp();
            openTimestamp = quote.getTimestamp();
        } else if (this.isin.equals(isin)) {
            if (Double.compare(quote.getPrice(), highPrice) > 0) {
                highPrice = quote.getPrice();
            }
            if (Double.compare(quote.getPrice(), highPrice) < 0) {
                lowPrice = quote.getPrice();
            }
            if (quote.getTimestamp().compareTo(closeTimestamp) > 0) {
                closeTimestamp = quote.getTimestamp();
                closePrice = quote.getPrice();
            }
        }
        return this;
    }

    public Long getCloseTimestamp() {
        return closeTimestamp;
    }

    public Long getOpenTimestamp() {
        return openTimestamp;
    }

    public Double getClosePrice() {
        return closePrice;
    }

    public Double getHighPrice() {
        return highPrice;
    }

    public Double getLowPrice() {
        return lowPrice;
    }

    public Double getOpenPrice() {
        return openPrice;
    }

    @Override
    public String toString() {
        return "AssetAggregation{" +
                "openTimestamp=" + openTimestamp +
                ", openPrice=" + openPrice +
                ", lowPrice=" + lowPrice +
                ", highPrice=" + highPrice +
                ", closePrice=" + closePrice +
                ", closeTimestamp=" + closeTimestamp +
                ", isin='" + isin + '\'' +
                '}';
    }
}

