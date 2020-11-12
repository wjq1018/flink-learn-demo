package com.hery.flink.java.pojo;

/**
 * @Date 2020/7/8 13:58
 * @Created by hery
 * @Description 股票价格 不是pojo对象
 */
public class StockPrice {
    public String symbol;
    public Long timestamp;
    public Double price;

    // 缺少无参数构造函数
    public StockPrice(String symbol, Long timestamp, Double price) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.price = price;
    }
}
