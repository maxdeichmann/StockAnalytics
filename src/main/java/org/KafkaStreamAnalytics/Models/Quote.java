package org.KafkaStreamAnalytics.Models;

public class Quote {
    private Long date;
    private Double price;

    public Quote() {

    }

    public Quote(Long timestamp, Double price) {
        this.date = timestamp;
        this.price = price;
    }

    public Long getTimestamp() {
        return this.date;
    }
    public Double getPrice() {
        return this.price;
    }



    @Override public String toString() {
        return "Quote(" + price + ", " + date + ")";
    }
}