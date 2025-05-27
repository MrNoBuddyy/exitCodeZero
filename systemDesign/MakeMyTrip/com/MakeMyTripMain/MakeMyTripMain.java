package com.MakeMyTripMain;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

class System{
    private List<Flight>flights;
    private static System system = null; // to make the class singleton
    public System(){}//make the constructor always public
    private static System getInstance(){
        if (system==null){
            system=new System();
        }
        return system;
    }
    public List<Flight> fetcFlights(String src, String dst, Date date){
        List<Flight>filteredFlights = new ArrayList<Flight>();
        for(Flight flight : flights){
            if(flight.date==date && flight.src==src && flight.dst==dst){
                filteredFlights.add(flight);
            }
        }
        return filteredFlights;
    }
    public boolean fixSeat(Aircraft aircraft, Seat seat){
        aircraft.fixSeat(seat,seat.getCustomer());
        return true;
    }
    public boolean addFlight( Flight flight){
        return true;
    }
    public boolean cancelFlight(Flight flight){
        return true;
    }
}
public class MakeMyTripMain {

    

}