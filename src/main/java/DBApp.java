import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;


public class DBApp implements DBAppInterface{

    Vector<Table> tableList;
    public DBApp (){
       Vector<Table> tableList= new Vector<Table>();
    }

    @Override
    public void init() {


    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                            Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException {

        FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv",true);
        csvWriter.write(getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax));
        csvWriter.flush();
        csvWriter.close();
        Table table = new Table(tableName,clusteringKey);
        this.tableList.add(table);
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public static String getAttributes ( String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                                         Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax){
        String columnName = "";
        String columnType ="";
        String columnMin ="";
        String columnMax ="";
        String clustering;
        String result="" ;
        Set<String> columns  = colNameType.keySet();
        for(String k : columns){

                    columnName = k;
                    columnType = colNameType.get(k);
                    columnMax = colNameMax.get(k);
                    columnMin = colNameMin.get(k);
                    if (clusteringKey.equals(k))
                        clustering = "TRUE";
                    else
                        clustering = "FALSE";

                    result = result + '\n' + tableName + "," + columnName + "," + columnType + "," + clustering + "," + "FALSE" + ","
                            + columnMin + "," + columnMax;

                }

        return result;


    }

 public static void main (String[] args) throws DBAppException, IOException {
       Hashtable table = new Hashtable<String,String>();

     table.put("Pen", "soso");
     table.put("Book", "lolo");
     table.put("Clothes", "momo");
     table.put("Mobile", "koko");
     table.put("Booklet", "hoho");

     Hashtable table1 = new Hashtable<String,String>();

     table1.put("Pen", "soso2");
     table1.put("Book", "lolo2");
     table1.put("Clothes", "momo2");
     table1.put("Mobile", "koko2");
     table1.put("Booklet", "hoho2");

     Hashtable table2 = new Hashtable<String,String>();

     table2.put("Pen", "soso3");
     table2.put("Book", "lolo3");
     table2.put("Clothes", "momo3");
     table2.put("Mobile", "koko3");
     table2.put("Booklet", "hoho3");

     DBApp db =new DBApp();

     db.createTable("marvelous","Pen",table,table1,table2);
    /* FileWriter cw = new FileWriter("src/main/resources/metadata.csv",true);
     String[] line = {"4", "David", "USA"};
     //Writing data to the csv file
    cw.write("lolo");
     //close the file
     cw.close();*/
}}
