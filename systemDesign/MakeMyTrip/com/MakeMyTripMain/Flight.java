package com.MakeMyTripMain;

import java.sql.Date;
import java.sql.Time;
import java.util.Map;

public class Flight {
    public String src;
    public String dst;
    public Date date;
    public Aircraft aircraft;
    public Time start;
    public Time end;
    public Flight(String src, String dst, Date date, Aircraft aircraft, Time start, Time end) {
        this.src = src;
        this.dst = dst;
        this.date = date;
        this.aircraft = aircraft;
        this.start = start;
        this.end = end;
    }
    public boolean cancelForCustomer(Customer c){
        Map<String,Seat>seats=aircraft.getSeats(); 
        for(Seat seat:seats.values()){
            if(seat.getCustomer()==c){
                seat.setCustomer(null);
            }
        }
        return true;
    }
    
}
