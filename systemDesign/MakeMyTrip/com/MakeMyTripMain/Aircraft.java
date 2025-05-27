package com.MakeMyTripMain;

import java.util.Map;

public class Aircraft {
    private Map<String,Seat> seats;

    public Map<String, Seat> getSeats() {
        return seats;
    }

    public boolean fixSeat(Seat seat, Customer customer) {
        seat.setCustomer(customer);
        return true;
    }
    

}
