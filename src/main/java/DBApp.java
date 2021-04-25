import java.io.*;
import java.util.*;


public class DBApp implements DBAppInterface{

    Vector<Table> tableList;
    public DBApp (){
       Vector<Table> tableList= new Vector<Table>();


    }

    @Override
    public void init() {

        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/Tables.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(tableList);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType,
                            Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException {
        Vector<String> colName=new Vector<>();
        try {
            String columnName = "";
            Set<String> columns = colNameType.keySet();
            for (String k : columns) {
                colName.add(k);
                columnName = k;
                if (!(colNameMax.containsKey(k) && colNameMin.containsKey(k)))
                    throw new DBAppException("invalid input");

            }
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv", true);
            csvWriter.write(getAttributes(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
            csvWriter.flush();
            csvWriter.close();
            Table table = new Table(tableName, clusteringKey);
            this.tableList.add(table);
        }
        catch(Exception e){
        System.out.print(e.getMessage());
        }
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
        Vector<Table> tables = null;
        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream("src/main/resources/Tables.bin");
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            tables = (Vector<Table>) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        Table target = null;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).tableName.equals(tableName)) {
                target = tables.get(i);
                break;
            }
            if (target == null)
                throw new DBAppException("Table not found");
        }
        String currenyClustering =colNameValue.get(target.clusteringKey).toString();

        //table has no pages case
        if (target.pages.size() == 0) {

            Page page = new Page(tableName + target.pages.size());
            checkDataTypes(tableName, colNameValue);
            page.list.add(colNameValue);
            serializePage(page);
            target.pages.add(page);
            target.pages.get(0).clusterings.add(colNameValue.get(target.clusteringKey).toString());
            target.min.add(colNameValue.get(target.clusteringKey).toString());
            target.max.add(colNameValue.get(target.clusteringKey).toString());
        }
        else{
            //case the no of pages the table has is only 1
            if (target.pages.size()==1){
                Page currentPage = target.pages.get(0);
                if(currentPage.list.size()<maxRows){
                    int x =0;
                    int idx = Collections.binarySearch(currentPage.clusterings,colNameValue.get(target.clusteringKey).toString());
                    if(idx>0)
                        throw new DBAppException("you entered a primary key that already exists");
                    else{
                        x= -1-idx;
                        if(currentPage.list.size()+1<maxRows){
                            currentPage.list.insertElementAt(colNameValue,x);
                            //inserting the clustering key at the clusterings vector
                            currentPage.clusterings.insertElementAt((String) colNameValue.get(target.clusteringKey.toString()),x);

                            if(currenyClustering.compareTo((String) target.min.get(0))==-1){
                                target.min.set(0,currenyClustering);
                            }
                            if(currenyClustering.compareTo(target.max.get(0))==1)
                                target.max.set(0,currenyClustering);
                        }
                        else{

                        }

                    }
                }
            }
        }
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

    public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        //  #####   parsing a CSV file into vector of String[]  #####

        Object min;
        Object max;
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i =0;
        try
        {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array= line.split(splitBy);
                Data.add(array);

            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
            for(int j =0 ;j<Data.size();j++){
                if(Data.get(j)[0].equals(tableName)) {
                    min = Data.get(j)[Data.get(j).length - 2];
                    String colName = Data.get(j)[1];
                    if (colNameValue.get(colName) != null) {
                        if (colNameValue.get(colName).toString().compareTo(min.toString()) == -1) {
                            throw new DBAppException("you entered value below minimum");
                        }
                        max = Data.get(j)[Data.get(j).length - 1];
                        if (colNameValue.get(colName).toString().compareTo(max.toString()) == 1) {
                            throw new DBAppException("you entered value above maximum");
                        }
                    }
                }
            }

        //  ######   looping over table attributes   #######
        String columnName="";
        Set<String> columns  = colNameValue.keySet();
        for(String k : columns){
            columnName = k;
            String dataType =getType(colNameValue.get(k));

            try {
                if (dataType.equals( "NA"))
                    throw new DBAppException("invalid Data Type");
                String CSVType="";

                for (int j=0;j<Data.size();j++){
                    String[] attributes =Data.get(j);
                    if (attributes[0].equals(tableName)&& attributes[1].equals(columnName))
                        CSVType =attributes[2];
                }
                if (!CSVType.equals(dataType))
                    throw new DBAppException("invalid Data Type");



            }catch (Exception e){
                System.out.print(e.getMessage());
            }



        }


    }

    public static String getType (Object o){
        if (o instanceof String)
            return "java.lang.String";
        else if(o instanceof Integer)
            return "java.lang.Integer";
        else if (o instanceof Double)
            return "java.lang.Double";
        else  if (o instanceof Date)
            return "java.util.Date";
        else
            return "NA";
    }

public static void serializePage(Page p){
        String pageName = p.PageID;
    try
    {
        //Saving of object in a file
        FileOutputStream file = new FileOutputStream("src/main/resources/PageID.bin");
        ObjectOutputStream out = new ObjectOutputStream(file);

        // Method for serialization of object
        out.writeObject(p);

        out.close();
        file.close();

        System.out.println("Object has been serialized");

    }

    catch(IOException ex)
    {
        System.out.println("IOException is caught");
    }

}

public int readConfig(String key) throws IOException {
        int nRows=0;
    FileReader reader=new FileReader("src/main/resources/DBApp.config");

    Properties p=new Properties();
    p.load(reader);
    return Integer.parseInt(p.getProperty(key));
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
   db.init();
     db.createTable("marvelous","Pen",table,table1,table2);

    /* FileWriter cw = new FileWriter("src/main/resources/metadata.csv",true);
     String[] line = {"4", "David", "USA"};
     //Writing data to the csv file
    cw.write("lolo");
     //close the file
     cw.close();*/
}}
