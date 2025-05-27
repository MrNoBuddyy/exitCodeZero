// Vehicle Classes
abstract class Vehicle {
    public abstract void printVehicle();
}

class TwoWheeler extends Vehicle {
    public void printVehicle() {
        System.out.println("I am two wheeler");
    }
}

class FourWheeler extends Vehicle {
    public void printVehicle() {
        System.out.println("I am four wheeler");
    }
}

// Factory Interface
interface VehicleFactory {
    Vehicle createVehicle();  // Abstract factory method
}

// Concrete Factory for TwoWheeler
class TwoWheelerFactory implements VehicleFactory {
    public Vehicle createVehicle() {
        return new TwoWheeler();  // Returns a TwoWheeler object
    }
}

// Concrete Factory for FourWheeler
class FourWheelerFactory implements VehicleFactory {
    public Vehicle createVehicle() {
        return new FourWheeler();  // Returns a FourWheeler object
    }
}

// Client Class
class Client {
    private Vehicle pVehicle;

    // The client takes any factory to create a vehicle
    public Client(VehicleFactory factory) {
        pVehicle = factory.createVehicle();  // Client gets any vehicle type via the factory
    }

    public void useVehicle() {
        pVehicle.printVehicle();  // Client can use the vehicle without knowing its type
    }
}

// Driver Program
public class VehicleMain {
    public static void main(String[] args) {
        // Client can use the same class for any type of vehicle
        VehicleFactory twoWheelerFactory = new TwoWheelerFactory();
        Client client1 = new Client(twoWheelerFactory);  // Same Client for TwoWheeler
        client1.useVehicle();  // Output: I am two wheeler

        VehicleFactory fourWheelerFactory = new FourWheelerFactory();
        Client client2 = new Client(fourWheelerFactory);  // Same Client for FourWheeler
        client2.useVehicle();  // Output: I am four wheeler
    }
}
