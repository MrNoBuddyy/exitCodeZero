package com.inventorysystem;

import java.util.HashMap;
import java.util.Map;

public class Order {
    Map<Product, Integer> productCount = new HashMap<>();

    public void addProduct(Product product, int quantity) {
        productCount.put(product, quantity);
    }
}
