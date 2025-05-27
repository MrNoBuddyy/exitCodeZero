package com.inventorysystem;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        // Step 1: Create some products
        Product p1 = new Product("P001", "Product 1", 100.0, 1.5, Size.M);
        Product p2 = new Product("P002", "Product 2", 200.0, 2.5, Size.L);

        // Step 2: Add products to the inventory system
        System.out.println("Adding products to the inventory...");
        User user = new User();
        user.addProduct(p1.id, p1.description, p1.price, p1.weight, p1.size);
        user.addProduct(p2.id, p2.description, p2.price, p2.weight, p2.size);

        // Step 3: Display the initial inventory status
        System.out.println("\nInitial Inventory Status:");
        displayInventoryStatus();

        // Step 4: Create an order
        Order order = new Order();
        order.addProduct(p1, 2); // 2 units of Product 1
        order.addProduct(p2, 1); // 1 unit of Product 2

        // Step 5: Execute the order using SimpleStrategy
        System.out.println("\nExecuting Order...");
        user.executeOrder(order, new SimpleStrategy());

        // Step 6: Display the remaining inventory after the order
        System.out.println("\nRemaining Inventory after Order Execution:");
        displayInventoryStatus();
    }

    // Helper method to display the current inventory status
    private static void displayInventoryStatus() {
        Map<Location, Unit> shelvesStatus = InventorySystem.getShelvesStatus();
        if (shelvesStatus.isEmpty()) {
            System.out.println("No units available in the inventory.");
        } else {
            for (Map.Entry<Location, Unit> entry : shelvesStatus.entrySet()) {
                Location location = entry.getKey();
                Unit unit = entry.getValue();
                if (unit != null) {
                    System.out.println("Location: " + location.id + ", Product: " + unit.productId + ", Status: " + unit.status);
                }
            }
        }
    }
}





// package com.inventorysystem;
// // import com.inventorysystem.{Product,Unit,User,Order};

// public class Main {
//     public static void main(String[] args) {
//         // Create products
//         Product p1 = new Product("P001", "Product 1", 100.0, 1.5, Size.M);
//         Product p2 = new Product("P002", "Product 2", 200.0, 2.5, Size.L);

//         // Add products to the inventory
//         User user = new User();
//         user.addProduct(p1.id, p1.description, p1.price, p1.weight, p1.size);
//         user.addProduct(p2.id, p2.description, p2.price, p2.weight, p2.size);

//         // Create an order
//         Order order = new Order();
//         order.addProduct(p1, 2);
//         order.addProduct(p2, 1);

//         // Execute order using SimpleStrategy
//         user.executeOrder(order, new SimpleStrategy());

//         // Show remaining inventory after the order
//         System.out.println("Remaining Inventory: " + InventorySystem.getShelvesStatus());
//     }
// }