package com.inventorysystem;

public class Unit {
    String id;
    String productId;
    String locationId;
    Status status;

    public Unit(String id, String productId, String locationId) {
        this.id = id;
        this.productId = productId;
        this.locationId = locationId;
        this.status = Status.INVENTORY;
    }
}
