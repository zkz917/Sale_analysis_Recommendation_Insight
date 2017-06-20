public class product implements Comparable<product>{
    private int productId;
    private double rating;

    public product(int productId, double rating) {
        this.productId = productId;
        this.rating = rating;
    }

    public int getproductId() {
        return productId;
    }
    public void setproductId(int productId) {
        this.productId = productId;
    }
    public double getRating() {
        return rating;
    }
    public void setRating(double rating) {
        this.rating = rating;
    }

    public int compareTo(product m) {
        double diff = rating - m.getRating();
        if(diff < 0) {
            return -1;
        } else if(diff > 0) {
            return 1;
        } else {
            return 0;
        }
    }
}
