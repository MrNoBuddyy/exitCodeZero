package vijay.builder.example.components;

public class GPSNavigator {
    private String route;

    
    public GPSNavigator(String manualRoute) {
        this.route = manualRoute;
    }
    public GPSNavigator() {
        this.route = "221b, Baker Street, London  to Scotland Yard, 8-10 Broadway, London";;
    }
    public String getRoute() {
        return route;
    }
}
