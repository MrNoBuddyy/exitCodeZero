package com.inventorysystem;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InventorySystem {
    static Map<String, Product> productMap = new HashMap<>();
    static Map<Location, Unit> locationMap = new HashMap<>();
    static Lock productLock = new ReentrantLock();
    static Lock locationLock = new ReentrantLock();

    // Add a product and create a corresponding unit
    public static void addProduct(Product product) {
        productLock.lock();
        try {
            productMap.put(product.id, product);

            // For simplicity, we'll assume each product goes to a location based on its size
            Location location = getLocationForProduct(product.size);
            Unit unit = new Unit(UUID.randomUUID().toString(), product.id, location.id);
            
            // Add the unit to the location map
            locationMap.put(location, unit);
        } finally {
            productLock.unlock();
        }
    }

    // Method to determine the location based on product size (for simplicity)
    private static Location getLocationForProduct(Size size) {
        Location location = new Location(UUID.randomUUID().toString(), size);
        return location;
    }

    // Place a unit in the inventory system
    public static void placeUnit(Unit unit) {
        locationLock.lock();
        try {
            // Place the unit in an empty location
            for (Map.Entry<Location, Unit> entry : locationMap.entrySet()) {
                if (entry.getValue() == null) {
                    unit.locationId = entry.getKey().id;
                    locationMap.put(entry.getKey(), unit);
                    return;
                }
            }
        } finally {
            locationLock.unlock();
        }
    }

    // Remove a unit (i.e., fulfill an order)
    public static void removeUnit(Product product) {
        locationLock.lock();
        try {
            Iterator<Map.Entry<Location, Unit>> iterator = locationMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Location, Unit> entry = iterator.next();
                if (entry.getValue() != null && entry.getValue().productId.equals(product.id)) {
                    iterator.remove();
                    return;
                }
            }
        } finally {
            locationLock.unlock();
        }
    }

    // Get the status of all shelves
    public static Map<Location, Unit> getShelvesStatus() {
        locationLock.lock();
        try {
            return new HashMap<>(locationMap);
        } finally {
            locationLock.unlock();
        }
    }

    public static Product getProduct(String productId) {
        productLock.lock();
        try {
            return productMap.get(productId);
        } finally {
            productLock.unlock();
        }
    }

    public static void updateStatus(Unit unit, Status status) {
        unit.status = status;
    }
}








// package com.inventorysystem;

// import java.util.*;
// import java.util.concurrent.locks.Lock;
// import java.util.concurrent.locks.ReentrantLock;

// public class InventorySystem {
//     static Map<String, Product> productMap = new HashMap<>();
//     static Map<Location, Unit> locationMap = new HashMap<>();
//     static Lock productLock = new ReentrantLock();
//     static Lock locationLock = new ReentrantLock();

//     public static void addProduct(Product product) {
//         productLock.lock();
//         try {
//             productMap.put(product.id, product);
//         } finally {
//             productLock.unlock();
//         }
//     }

//     public static void placeUnit(Unit unit) {
//         locationLock.lock();
//         try {
//             for (Map.Entry<Location, Unit> entry : locationMap.entrySet()) {
//                 if (entry.getValue() == null) {
//                     unit.locationId = entry.getKey().id;
//                     locationMap.put(entry.getKey(), unit);
//                     return;
//                 }
//             }
//         } finally {
//             locationLock.unlock();
//         }
//     }

//     public static void removeUnit(Product product) {
//         locationLock.lock();
//         try {
//             Iterator<Map.Entry<Location, Unit>> iterator = locationMap.entrySet().iterator();
//             while (iterator.hasNext()) {
//                 Map.Entry<Location, Unit> entry = iterator.next();
//                 if (entry.getValue() != null && entry.getValue().productId.equals(product.id)) {
//                     iterator.remove();
//                     return;
//                 }
//             }
//         } finally {
//             locationLock.unlock();
//         }
//     }

//     public static Product getProduct(String productId) {
//         productLock.lock();
//         try {
//             return productMap.get(productId);
//         } finally {
//             productLock.unlock();
//         }
//     }

//     public static Map<Location, Unit> getShelvesStatus() {
//         locationLock.lock();
//         try {
//             return new HashMap<>(locationMap);
//         } finally {
//             locationLock.unlock();
//         }
//     }

//     public static void updateStatus(Unit unit, Status status) {
//         unit.status = status;
//     }
// }
