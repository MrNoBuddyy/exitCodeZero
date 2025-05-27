package vijay.builder.example.builders;
import vijay.builder.example.cars.Car;
import vijay.builder.example.cars.CarType;
import vijay.builder.example.components.Engine;
import vijay.builder.example.components.GPSNavigator;
import vijay.builder.example.components.Transmission;
import vijay.builder.example.components.TripComputer;
public class CarBuilder implements Builder {
    private CarType type;
    private int seats;
    private Engine engine;
    private Transmission transmission;
    private TripComputer tripComputer;
    private GPSNavigator gpsNavigator;
    public void setCarType(CarType type) {
        this.type = type;
    }
    @Override
    public void setSeats(int seats){
        this.seats=seats;
    }
    @Override
    public void setEngine(Engine engine){
        this.engine=engine;
    }
    @Override
    public void setTransmission(Transmission transmission){
        this.transmission=transmission;
    }
    @Override
    public void setTripComputer(TripComputer tripComputer){
        this.tripComputer =tripComputer;
    }
    @Override
    public void setGPSNavigator(GPSNavigator gpsNavigator){
        this.gpsNavigator=gpsNavigator;
    }
    public Car getResult(){
        return new Car(type,seats,engine,transmission,tripComputer,gpsNavigator);
    }
}
