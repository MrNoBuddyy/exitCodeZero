package vijay.builder.example.builders;
import vijay.builder.example.cars.CarType;
import vijay.builder.example.components.Engine;
import vijay.builder.example.components.GPSNavigator;
import vijay.builder.example.components.Transmission;
import vijay.builder.example.components.TripComputer;

public interface Builder {
    void setCarType(CarType type);
    void setSeats(int seats);
    void setEngine(Engine engine);
    void setTransmission(Transmission transmission);
    void setTripComputer(TripComputer tripComputer);
    void setGPSNavigator(GPSNavigator gpsNavigator);

}
