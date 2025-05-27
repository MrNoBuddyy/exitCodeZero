file://<WORKSPACE>/Main.java
### java.util.NoSuchElementException: next on empty iterator

occurred in the presentation compiler.

presentation compiler configuration:


action parameters:
offset: 110
uri: file://<WORKSPACE>/Main.java
text:
```scala
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@@class InventorySystem {
    static Map<String, Product> productMap = new HashMap<>();
    static Map<Location, Unit> locationMap = new HashMap<>();
    static Lock productLock = new ReentrantLock();
    static Lock locationLock = new ReentrantLock();

    // Add a product to the inventory
    public static void addProduct(Product product) {
        productLock.lock();
        try {
            productMap.put(product.id, product);
        } finally {
            productLock.unlock();
        }
    }

    // Place a unit into a location
    public static void placeUnit(Unit unit) {
        locationLock.lock();
        try {
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

    // Remove a unit from inventory (after an order is executed)
    public static void removeUnit(Product product) {
        locationLock.lock();
        try {
            Iterator<Map.Entry<Location, Unit>> iterator = locationMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Location, Unit> entry = iterator.next();
                if (entry.getValue() != null && entry.getValue().productId.equals(product.id)) {
                    iterator.remove();  // Remove unit from locationMap
                    return;
                }
            }
        } finally {
            locationLock.unlock();
        }
    }

    // Get product details by product ID
    public static Product getProduct(String productId) {
        productLock.lock();
        try {
            return productMap.get(productId);
        } finally {
            productLock.unlock();
        }
    }

    // Get the current status of all shelves/locations
    public static Map<Location, Unit> getShelvesStatus() {
        locationLock.lock();
        try {
            return new HashMap<>(locationMap);  // Returning a copy to avoid external modification
        } finally {
            locationLock.unlock();
        }
    }

    // Update the status of a unit
    public static void updateStatus(Unit unit, Status status) {
        unit.status = status;
    }
}

// Product class representing individual products
class Product {
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

// Unit class representing units of products at locations
class Unit {
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

// Location class to represent warehouse locations
class Location {
    String id;
    Size size;

    public Location(String id, Size size) {
        this.id = id;
        this.size = size;
    }
}

// Enum representing the different statuses of units
enum Status {
    INVENTORY, TRANSIT, DELIVERY
}

// Enum representing sizes of products
enum Size {
    S, M, L
}

// User class for managing operations such as adding products and executing orders
class User {
    public void addProduct(String id, String description, double price, double weight, Size size) {
        InventorySystem.addProduct(new Product(id, description, price, weight, size));
    }

    public void executeOrder(Order order, IStrategy strategy) {
        strategy.execute(order);
    }
}

// Order class containing a map of products and their quantities
class Order {
    Map<Product, Integer> productCount = new HashMap<>();

    public void addProduct(Product product, int quantity) {
        productCount.put(product, quantity);
    }
}

// Strategy interface for order processing
interface IStrategy {
    void execute(Order order);
}

// SimpleStrategy that uses a basic approach (e.g., first available unit)
class SimpleStrategy implements IStrategy {
    @Override
    public void execute(Order order) {
        for (Map.Entry<Product, Integer> item : order.productCount.entrySet()) {
            Product product = item.getKey();
            int quantity = item.getValue();
            for (int i = 0; i < quantity; i++) {
                // Try to remove the unit of the product from the inventory
                InventorySystem.removeUnit(product);
            }
        }
    }
}

// SmartStrategy that uses more advanced logic (e.g., prioritizes units in certain locations)
class SmartStrategy implements IStrategy {
    @Override
    public void execute(Order order) {
        for (Map.Entry<Product, Integer> item : order.productCount.entrySet()) {
            Product product = item.getKey();
            int quantity = item.getValue();

            // In SmartStrategy, we can implement advanced logic for unit allocation
            for (int i = 0; i < quantity; i++) {
                // Smart logic could prioritize units based on certain criteria, like location
                InventorySystem.removeUnit(product);
            }
        }
    }
}

// Test implementation
public class Main {
    public static void main(String[] args) {
        // Create products
        Product p1 = new Product("P001", "Product 1", 100.0, 1.5, Size.M);
        Product p2 = new Product("P002", "Product 2", 200.0, 2.5, Size.L);

        // Add products to the inventory
        User user = new User();
        user.addProduct(p1.id, p1.description, p1.price, p1.weight, p1.size);
        user.addProduct(p2.id, p2.description, p2.price, p2.weight, p2.size);

        // Create an order
        Order order = new Order();
        order.addProduct(p1, 2);
        order.addProduct(p2, 1);

        // Execute order using SimpleStrategy
        user.executeOrder(order, new SimpleStrategy());

        // Show remaining inventory after the order
        System.out.println("Remaining Inventory: " + InventorySystem.getShelvesStatus());
    }
}

```



#### Error stacktrace:

```
scala.collection.Iterator$$anon$19.next(Iterator.scala:973)
	scala.collection.Iterator$$anon$19.next(Iterator.scala:971)
	scala.collection.mutable.MutationTracker$CheckedIterator.next(MutationTracker.scala:76)
	scala.collection.IterableOps.head(Iterable.scala:222)
	scala.collection.IterableOps.head$(Iterable.scala:222)
	scala.collection.AbstractIterable.head(Iterable.scala:935)
	dotty.tools.dotc.interactive.InteractiveDriver.run(InteractiveDriver.scala:164)
	dotty.tools.pc.MetalsDriver.run(MetalsDriver.scala:45)
	dotty.tools.pc.HoverProvider$.hover(HoverProvider.scala:40)
	dotty.tools.pc.ScalaPresentationCompiler.hover$$anonfun$1(ScalaPresentationCompiler.scala:376)
```
#### Short summary: 

java.util.NoSuchElementException: next on empty iterator