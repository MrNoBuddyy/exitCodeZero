package com.MakeMyTripMain;

public class Seat {
    private SEAT type;
    private String id;
    public Customer customer;
    private int price;
    public Customer getCustomer() {
        return customer;
    }

    public boolean setCustomer(Customer customer) {
        if(this.getCustomer()==null){
            this.customer=customer;
            return true;
        }
        else return false;
    }
    
}
enum SEAT{
    EMERGENCY, LUXUARY,NORMAL
}
