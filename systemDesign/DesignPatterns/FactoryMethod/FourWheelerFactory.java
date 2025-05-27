package FactoryMethod;

class FourWheelerFactory implements VehicleFactory {
    public Vehicle createVehicle(){
        return new FourWheeler();
    }
}
