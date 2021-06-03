import java.io.*;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Vector;

public class Index {
    String tableName;
    String[] colNames;
    Hashtable<String,Vector<String>> ranges;
    String indexId;
    Vector grid;
    public Index(String tableName, String[] columnNames){
        this.tableName=tableName;
        colNames=columnNames;
        grid = new Vector();
        indexId="IDX_"+tableName;
        for (int i=0;i<columnNames.length;i++){
            indexId+="_"+columnNames[i];
        }
    ranges=new Hashtable<String,Vector<String>>();
        for (int i=0;i<colNames.length;i++)
            ranges.put(columnNames[i],new Vector());
    }
    public void serializeIndex(){
        String indexId = this.indexId;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+indexId+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(this);

            out.close();
            file.close();
            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage());
        }

    }
    public  Index DeserializeIndex(String path){
        Index i = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            i = (Index) in.readObject();
            in.close();
            file.close();

            //  System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
        }
        return i;}


}
