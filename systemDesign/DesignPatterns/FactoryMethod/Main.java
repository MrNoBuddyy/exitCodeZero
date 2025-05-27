package FactoryMethod;

public class Main {
    public static void main(String[] args) {
        VehicleFactory twVehicleFactory = new TwoWheelerFactory();
        Client twoWheelerClient = new Client(twVehicleFactory);
        Vehicle twoWheeler = twoWheelerClient.getVehicle();
        twoWheeler.printVehicle();
        VehicleFactory fouVehicleFactory = new FourWheelerFactory();
        Client fourWheelerClient = new Client(fouVehicleFactory);
        Vehicle fourWheeler = fourWheelerClient.getVehicle();
        fourWheeler.printVehicle();
    }
}
