import java.io.*;

abstract class Vehicle{
    public abstract void printVehicle();
}
class TwoWheeler extends Vehicle{
    public void printVehicle(){
        System.out.println("I am a two wheeler");
    }
}
class FourWheeler extends Vehicle{
    public void printVehicle(){
        System.out.println("I am a four Wheeler");
    }
}
class Client{
    private Vehicle pVehicle;
    public Client(int type){
        if(type==1){
            pVehicle = new TwoWheeler();
        }else if (type==2){
            pVehicle = new FourWheeler();
        }else{
            pVehicle=null;
        }
    }
    public  void cleanUp(){
        if(pVehicle!=null){
            pVehicle=null;
        }
    }
    public Vehicle getVehicle(){
        return pVehicle;
    }

}

public class VehicleFactoryPattern {
    public static void main(String [] args){
        Client pClient = new Client(1);
        Vehicle pVehicle = pClient.getVehicle();
        if(pVehicle!=null){
            pVehicle.printVehicle();
        }
        pClient.cleanUp();
        pClient = new Client(2);
        Vehicle pVehicle2 = pClient.getVehicle();
        pVehicle2.printVehicle();


    }
}