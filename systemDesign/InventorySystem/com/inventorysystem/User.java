package com.inventorysystem;

public class User {
    public void addProduct(String id, String description, double price, double weight, Size size) {
        InventorySystem.addProduct(new Product(id, description, price, weight, size));
    }

    public void executeOrder(Order order, IStrategy strategy) {
        strategy.execute(order);
    }
}
