package com.inventorysystem;

public class Product {
    String id;
    String description;
    double price;
    double weight;
    Size size;

    public Product(String id, String description, double price, double weight, Size size) {
        this.id = id;
        this.description = description;
        this.price = price;
        this.weight = weight;
        this.size = size;
    }
}
