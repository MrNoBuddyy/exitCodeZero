package vijay.builder.example;
import vijay.builder.example.builders.CarBuilder;
import vijay.builder.example.builders.CarManualBuilder;
import vijay.builder.example.cars.Car;
import vijay.builder.example.cars.Manual;
import vijay.builder.example.director.Director;
public class Demo {
    public static void main(String args[]){
        Director director = new Director();
        CarBuilder builder = new CarBuilder();
        director.constructSportsCar(builder);
        Car car = builder.getResult();
        System.out.println("Car built:\n" + car.getCarType());
        CarManualBuilder manualBuilder = new CarManualBuilder();
        director.constructSportsCar(manualBuilder);
        Manual manual = manualBuilder.getResult();
        System.out.println("\nCar manual built:\n" + manual.print());
    }
}
