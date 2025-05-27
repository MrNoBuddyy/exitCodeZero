package FactoryMethod;

class TwoWheelerFactory implements VehicleFactory {
    public Vehicle createVehicle(){
        return new TwoWheeler();
    }
}
