public class CustomerGenerator {
    static int id = 0;

    static Customer getNext() {
        id++;
        return new Customer(id, "name " + id);
    }
}
