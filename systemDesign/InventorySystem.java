import java.util.HashMap;
import java.util.Map;

public class InventorySystem {
    static Map<String, Product> productMap = new HashMap<>();
    static Map<Location, Unit> locationMap = new HashMap<>();

    public static void addProduct(Product product) {
        productMap.put(product.id, product);
    }

    public static void placeUnit(Unit unit) {
        for(Map.Entry<Location,Unit>entry:locationMap.entrySet()){
            //get lock on entry.getKey()
            if(entry.getValue()==null){
                unit.locationId=entry.getKey().id;
            }
            //release lock on entry.getKey()
        } 
    }
    public static void removeUnit(Product product) {
        for(Map.Entry<Location,Unit>entry:locationMap.entrySet()){
            //if(simpleStrategy works)
            //get lock on entry.getKey()
            if(entry.getValue()==null && product.id.equals(entry.getKey().id)){
                locationMap.remove(entry.getKey());
            }
            //release lock on entry.getKey()
        } 
    }
    public static Product getProduct(String product) {
        return productMap.get(product);
    }

    public static Map<Location, Unit> getShalvesStatus() {
        return locationMap;
    }

    public static void updateStatus(Unit unit, Status status) {
        unit.status = status;
    }

}

class Unit {
    String id;
    String productId;
    String locationId;
    Status status;
}

class Location {
    String id;
    Size size;
}

enum Status {
    INVENTORY, TRANSIT, DELIVERY
}

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

enum Size {
    S, M, L
}

class User {
    public void addProduct(){
        InventorySystem.addProduct(new Product("","",0,0,Size.L));
    }
    public void execureOrder(Order order){
        for(Map.Entry<Product, Integer>item:order.producCount.entrySet()){
            for(int i=0;i<item.getValue();i++){
        InventorySystem.removeUnit(item.getKey());}}
    }

}
class SimpleStrategy{}
class SmartStrategy{}

class Order {
    Map<Product, Integer> producCount = new HashMap<>();

}