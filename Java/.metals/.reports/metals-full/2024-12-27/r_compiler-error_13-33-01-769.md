file://<WORKSPACE>/Main.java
### java.util.NoSuchElementException: next on empty iterator

occurred in the presentation compiler.

presentation compiler configuration:


action parameters:
offset: 34
uri: file://<WORKSPACE>/Main.java
text:
```scala


// Test implementation

import S@@

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
	dotty.tools.pc.completions.CompletionProvider.completions(CompletionProvider.scala:50)
	dotty.tools.pc.ScalaPresentationCompiler.complete$$anonfun$1(ScalaPresentationCompiler.scala:146)
```
#### Short summary: 

java.util.NoSuchElementException: next on empty iterator