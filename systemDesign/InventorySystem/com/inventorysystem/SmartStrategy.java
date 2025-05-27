package com.inventorysystem;

import java.util.Map;

public class SmartStrategy implements IStrategy {
    @Override
    public void execute(Order order) {
        for (Map.Entry<Product, Integer> item : order.productCount.entrySet()) {
            Product product = item.getKey();
            int quantity = item.getValue();

            // You can add more advanced logic here
            for (int i = 0; i < quantity; i++) {
                InventorySystem.removeUnit(product);
            }
        }
    }
}
