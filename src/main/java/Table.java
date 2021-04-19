import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    transient Vector<Page> pages;
    String clusteringKey;
    public Table (String name,String clustering){
        tableName = name;
        pages = new Vector<Page>();
        clusteringKey = clustering;


    }
}
