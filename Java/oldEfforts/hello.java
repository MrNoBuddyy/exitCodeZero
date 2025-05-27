class Box {
    private int length, height, width;

    public void setDimentions(int length, int height, int width) {
        this.height = height;
        this.length = length;
        this.width = width;
    }

    public int boxArea() {
        int area = 2 * (this.height * this.width + this.height * this.length + this.length * this.width);
        return area;
    }

    public int boxVolume() {
        int volume = this.height * this.width * this.length;
        return volume;
    }
}

class hello {
    public static void main(String[] args) {
        Box box = new Box();
        box.setDimentions(2, 3, 4);
        System.out.println(box.boxArea());
        System.out.println(box.boxVolume());
        System.out.println(box.boxArea());
        System.out.println(box.boxVolume());
        System.out.println("HelloBuddy");
    }
}