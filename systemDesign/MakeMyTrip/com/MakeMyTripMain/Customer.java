package com.MakeMyTripMain;

public class Customer {
    private String id;
    private String name;
    private System system;
    public boolean fixSeat(Flight flight,Seat seat){
        return flight.aircraft.fixSeat(seat, this);
    }
    public boolean cancelBooking(Flight flight){
        flight.cancelForCustomer(this);
        return true;
    }
}
