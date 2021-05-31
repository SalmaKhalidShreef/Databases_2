import org.junit.jupiter.api.parallel.Resources;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

///Final
public class DBApp implements DBAppInterface {

    //private static Object String;
    Vector<String> tableList;
    Vector<String> tableNames;


    public DBApp (){
        tableList= new Vector<String>();
        tableNames = new Vector<String>();

    }
    private static final String FOLDER ="src/main/resources/data";

    @Override
    public void init() {
        FileWriter csvWriter = null;
        //File data = new File("src/main/resources");
        File newFolder = new File(FOLDER);

        boolean created =  newFolder.mkdir();

        if(created)
            System.out.println("Folder was created !");
        else
            System.out.println("shhhh");

        String title = "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max";
        try {
            csvWriter = new FileWriter("src/main/resources/metadata.csv", true);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            csvWriter.write(title);
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*try
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
        }*/
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
            Vector <String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
            if(tableNames.contains(tableName))
                throw new DBAppException("A table with this name already exists");
            Vector <String> tableList = deserializeVector("src/main/resources/data/tablesList.bin");
            tableList.add("src/main/resources/data/"+tableName+".bin");
            serializetableList(tableList);

            tableNames.add(tableName);
            serializetableNames(tableNames);
            serializeTable(table);

        }
        catch(Exception e){
            System.out.print(e.getMessage());
        }
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    }
    /*    FileOutputStream fout=new FileOutputStream("src/main/resources/metadata.csv");
        String data =getAttributes(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        fout.write(data.getBytes(), 0, data.length());
*/
    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Index index = new Index(tableName,columnNames);
        Vector indicies = deserializeVector("src/main/resources/data/indicies.bin");
        indicies.add("src/main/resources/data/"+index.indexId+".bin");
        serializeindicies(indicies);
        buildArray(index.colNames.length, index.grid);
        updateMetadata(columnNames,tableName);
        createRanges(index);
        //method salma w nouran
        loopPages(tableName,columnNames,index);
        index.serializeIndex();


    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        colNotFound(tableName, colNameValue);
        Vector<String> tables = null; //all tables in the DB
        Table target = null;
        String tableFilePath = null;
        String currentClustering = null; //clusteringKey of the entry
        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinPage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        tables = deserializeVector("src/main/resources/data/tablesList.bin");
        Vector<String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        //getting targetted table
        for (int i = 0; i < tables.size(); i++) {
            if (tableNames.get(i).compareTo(tableName) == 0) {
                tableFilePath = tables.get(i);
                target = DeserializeTable(tableFilePath);
                break;
            }
        }
        if (target == null)
            throw new DBAppException("Table not found");
        currentClustering = colNameValue.get(target.clusteringKey).toString();
        if (currentClustering == null) {
            throw new DBAppException("you entered an entry with no primary key");
        }
        //table has no pages case
        if (target.pagesPath.size() == 0) {
            Page page = new Page(tableName + "0");
            try {
                checkDataTypes(tableName, colNameValue);
                page.list.add(colNameValue);
                page.clusterings.add(colNameValue.get(target.clusteringKey).toString());
                serializePage(page);
                //"src/main/resources/data/"+pageName+".bin"
                target.pagesPath.add("src/main/resources/data/" + page.PageID + ".bin");
                target.min.add(colNameValue.get(target.clusteringKey).toString());
                target.max.add(colNameValue.get(target.clusteringKey).toString());
                serializeTable(target);
                serializePage(page);
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }//end of no pages case
        else {
            //finding the target page
            int pageIndex = 0;
            int k;
            for (k = 0; k < target.pagesPath.size(); k++) {
                if (currentClustering.compareTo(target.min.get(k).toString()) < 0) {
                    String currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                    Page currentPage = deserialize(currentPath);
                    if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                        throw new DBAppException("there is an entry with this primary Key !");
                    if (currentPage.list.size() < maxRows) {
                        insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                        serializeTable(target);
                        serializePage(currentPage);
                        return;
                    }
                    //I reached max rows
                    else {
                        // check for existance of next page should be added
                        //I have a next page case
                        String nextPath = "src/main/resources/data" + "/" + target.tableName + (k + 1) + ".bin";
                        if (target.pagesPath.contains(nextPath)) {
                            Page nextPage = deserialize(nextPath);
                            // next page below max rows
                            if (nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                            if (nextPage.list.size() < maxRows) {
                                Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                target.max.set(k, newMax);
                                currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                nextPage.list.insertElementAt(targetElement, 0);
                                nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                target.min.set(k + 1, (targetElement.get(target.clusteringKey)).toString());
                                insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                serializePage(nextPage);
                                serializePage(currentPage);
                                serializeTable(target);
                            } else {
                                currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                currentPage = deserialize(currentPath);
                                if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                if (currentPage.overflowPage == null)
                                    currentPage.overflowPage = new Page(currentPage.PageID + "Over");
                                insertIntoPage(currentPage.overflowPage, colNameValue, currentClustering, target, k);
                                serializePage(currentPage.overflowPage);
                                serializePage(currentPage);
                                serializeTable(target);
                                return;
                            }
                        }//I dont have nextPage
                        // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                        else {
                            Page newPage = new Page(target.tableName + target.pagesPath.size());
                            target.pagesPath.add("src/main/resources/data" + "/" + newPage.PageID + ".bin");
                            currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                            currentPage = deserialize(currentPath);
                            if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                throw new DBAppException("there is an entry with this primary Key !");
                            Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                            currentPage.list.removeElementAt(currentPage.list.size() - 1);
                            String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                            target.max.set(k, newMax);
                            currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                            newPage.list.insertElementAt(targetElement, 0);
                            newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                            target.min.add(targetElement.get(target.clusteringKey).toString());
                            insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                            target.max.add(targetElement.get(target.clusteringKey).toString());
                            serializePage(newPage);
                            serializePage(currentPage);
                            serializeTable(target);
                            return ;
                        }
                        //end of else -Iam not below max rows-
                    }
                }//end of I am below min case
                else{
                    if (currentClustering.compareTo(target.min.get(k).toString()) > 0 && currentClustering.compareTo(target.max.get(k).toString()) < 0) {
                        String currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                        Page currentPage = deserialize(currentPath);
                        if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                            throw new DBAppException("there is an entry with this primary Key !");
                        if (currentPage.list.size() < maxRows) {
                            insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                            serializeTable(target);
                            serializePage(currentPage);
                            return;
                        }//No I reached max
                        else {
                            // check for existance of next page should be added
                            //I have a next page case
                            String nextPath = "src/main/resources/data" + "/" + target.tableName + (k + 1) + ".bin";
                            if (target.pagesPath.contains(nextPath)) {
                                Page nextPage = deserialize(nextPath);
                                // next page below max rows
                                if (nextPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                if (nextPage.list.size() < maxRows) {
                                    Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                    currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                    String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                    target.max.set(k, newMax);
                                    currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                    nextPage.list.insertElementAt(targetElement, 0);
                                    nextPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                    target.min.set(k + 1, targetElement.get(target.clusteringKey).toString());
                                    insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                    serializePage(nextPage);
                                    serializePage(currentPage);
                                    serializeTable(target);
                                } else {
                                    currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                    currentPage = deserialize(currentPath);
                                    if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                        throw new DBAppException("there is an entry with this primary Key !");
                                    if (currentPage.overflowPage == null)
                                        currentPage.overflowPage = new Page(currentPage.PageID + "Over");
                                    insertIntoPage(currentPage.overflowPage, colNameValue, currentClustering, target, k);
                                    serializePage(currentPage.overflowPage);
                                    serializePage(currentPage);
                                    serializeTable(target);
                                    return;
                                }
                            }//I dont have nextPage
                            // in CASE THAT THE CURRENT PAGE IS THE LAST PAGE , THEN WE CREATE NEW PAGE AND SHIFT ONE ROW DOWN
                            else {
                                Page newPage = new Page(target.tableName + target.pagesPath.size());
                                target.pagesPath.add("src/main/resources/data" + "/" + newPage.PageID + ".bin");
                                currentPath = "src/main/resources/data" + "/" + target.tableName + k + ".bin";
                                currentPage = deserialize(currentPath);
                                if (currentPage.clusterings.contains(colNameValue.get(target.clusteringKey).toString()))
                                    throw new DBAppException("there is an entry with this primary Key !");
                                Hashtable targetElement = currentPage.list.get(currentPage.list.size() - 1);
                                currentPage.list.removeElementAt(currentPage.list.size() - 1);
                                String newMax = currentPage.list.get(currentPage.list.size() - 1).get(target.clusteringKey).toString();
                                target.max.set(k, newMax);
                                currentPage.clusterings.removeElementAt(currentPage.clusterings.size() - 1);
                                newPage.list.insertElementAt(targetElement, 0);
                                newPage.clusterings.insertElementAt(targetElement.get(target.clusteringKey).toString(), 0);
                                target.min.add(targetElement.get(target.clusteringKey).toString());
                                insertIntoPage(currentPage, colNameValue, currentClustering, target, k);
                                target.max.add(targetElement.get(target.clusteringKey).toString());
                                serializePage(newPage);
                                serializePage(currentPage);
                                serializeTable(target);
                                return ;
                            }
                            //end of else -Iam not below max rows-
                        }
                    }
                    }
            }//end of loop
            if (k >= target.pagesPath.size()) {
                String path = target.pagesPath.get(target.pagesPath.size() - 1);
                Page lastPage = deserialize(path);
                if (lastPage.list.size() < maxRows) {
                    insertIntoPage(lastPage,colNameValue,currentClustering,target,target.pagesPath.size()-1);
                    serializeTable(target);
                    return;
                }
                else {
                    Page newPage = new Page(tableName + k);
                    newPage.list.add(colNameValue);
                    newPage.clusterings.add(currentClustering);
                    target.max.add(currentClustering);
                    target.min.add(currentClustering);
                    target.pagesPath.add("src/main/resources/data/" + newPage.PageID + ".bin");
                    serializePage(newPage);
                    serializeTable(target);
                    return;
                }
            }

        }    }
        public static void insertIntoPage (Page currentPage, Hashtable colNameValue, String currentClustering, Table
        target,int pageIndex) throws DBAppException {
            int x = 0;
            int idx = Collections.binarySearch(currentPage.clusterings, colNameValue.get(target.clusteringKey).toString());
            if(currentPage ==null)
            System.out.println("ANA HENA AHO");
           if(currentPage.clusterings.size()!=currentPage.list.size()){ System.out.println(currentPage.clusterings.size() +"clusterings");
            System.out.println(currentPage.list.size()+"list");

           System.out.println(currentPage.clusterings.toString());
           System.out.println(currentPage.list.toString());}

            x = -1 - idx;
                if (x > 250){
                    throw  new DBAppException("Ana bayez xX");
                    }
                currentPage.list.insertElementAt(colNameValue, x);
                //inserting the clustering key at the clusterings vector
                currentPage.clusterings.insertElementAt( colNameValue.get(target.clusteringKey.toString()).toString(), x);

                if (currentClustering.compareTo(target.min.get(pageIndex).toString()) < 0) {
                    target.min.set(pageIndex, currentClustering);
                }
                if (currentClustering.compareTo(target.max.get(pageIndex).toString()) > 0)
                    target.max.set(pageIndex, currentClustering);
                serializePage(currentPage);
                serializeTable(target);
        }


        @Override
        public void updateTable (String tableName, String
        clusteringKeyValue, Hashtable < String, Object > columnNameValue) throws DBAppException {

            colNotFound(tableName, columnNameValue);
            try {
                checkDataTypes(tableName, columnNameValue);
                int i;

                // String s="s/"
                Boolean flag = false;

                Table table = null;
                Vector<String> tableList = (Vector<String>) deserializeVector("src/main/resources/data/tablesList.bin");
                for (i = 0; i < tableList.size(); i++) {
                    table = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
                    if (table.tableName.equals(tableName)) {
                        if (columnNameValue.get(table.clusteringKey) != null)
                            throw new DBAppException("You can't update clustering key !");
                        flag = true;
                        break;
                    }


                }
                if (flag) {
                    Boolean flagfoundpage = false;
                    int idx = -1;
                    Page page;
                    int j;
                    for (j = 0; j < table.min.size(); j++) {
                        if (clusteringKeyValue.compareTo((table.min.get(j)).toString()) >= 0 &&
                                clusteringKeyValue.compareTo((table.max.get(j)).toString()) <= 0) {
                            page = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) + ".bin");
                            //idx = Collections.binarySearch(page.clusterings,clusteringKeyValue);
                            if (!page.clusterings.contains(clusteringKeyValue)) {
                                if (page.overflowPage != null) {
                                    Page overflow = deserialize("src/main/resources/data" + "/" + table.tableName + (j + 1) + "Over" + ".bin");
                                    if (overflow.clusterings.contains(clusteringKeyValue)) {
                                        String s = (String) (columnNameValue.keySet().toArray())[0];
                                        columnNameValue.replace(s, columnNameValue.get(s));
                                        flagfoundpage = true;
                                        System.out.println("updating in overFloe");

                                        break;
                                    }
                                }
                            } else if (page.clusterings.contains(clusteringKeyValue)) {
                                String s = (String) (columnNameValue.keySet().toArray())[0];
                                columnNameValue.replace(s, columnNameValue.get(s));
                                flagfoundpage = true;
                            }

                            serializePage(page);
                            break;
                        }
                    }
                    if (!flagfoundpage) {
                        throw new DBAppException("Row not found");
                    }

                } else {
                    throw new DBAppException("The table does not exist");
                }
                serializeTable(table);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        }

        @Override
        public void deleteFromTable (String tableName, Hashtable < String, Object > columnNameValue) throws
        DBAppException {
            // colNotFound(tableName,columnNameValue);
            Page p = getPage(tableName, columnNameValue);
            if (p != null) {
                try {
                    Table t = DeserializeTable("src/main/resources/data/" + tableName + ".bin");
                    int rowIdx = getRow(p, columnNameValue, tableName);
                    String cluster = p.list.get(rowIdx).get(t.clusteringKey).toString();
                    p.list.removeElementAt(rowIdx);
                    p.clusterings.removeElementAt(rowIdx);
                    char pid = p.PageID.charAt(tableName.length());
                    int idx = Character.getNumericValue(pid);
                    if (cluster.equals(t.min.get(idx))) {
                        t.min.set(idx, p.clusterings.get(0));
                    }

                    if (cluster.equals(t.max.get(idx))) {
                        t.max.removeElementAt(idx);
                        t.max.set(idx, p.clusterings.get(p.clusterings.size() - 1));
                    }
                    serializeTable(t);
                } catch (Exception x) {
                    System.out.println(x.getMessage());
                }
            }
        }

        @Override
        public Iterator selectFromTable (SQLTerm[]sqlTerms, String[]arrayOperators) throws DBAppException {
            return null;
        }

        public static String getAttributes (String tableName, String
        clusteringKey, Hashtable < String, String > colNameType,
                Hashtable < String, String > colNameMin, Hashtable < String, String > colNameMax){
            String columnName = "";
            String columnType = "";
            String columnMin = "";
            String columnMax = "";
            String clustering;
            String result = "";
            Set<String> columns = colNameType.keySet();
            for (String k : columns) {

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


    public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, ParseException {

        //  #####   parsing a CSV file into vector of String[]  #####

        String min;
        String max;
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
        boolean tableFound = false;
        boolean colFound = false;
        for(int j =0 ;j<Data.size();j++){
            if(Data.get(j)[0].equals(tableName)) {
                min = Data.get(j)[Data.get(j).length - 2];
                tableFound=true;
                String colName = Data.get(j)[1];
                if (colNameValue.get(colName) != null) {
                    colFound= true;
                    if(colNameValue.get(colName) instanceof  Date){
                        Date d = (Date)colNameValue.get(colName);
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
                        String strDate = dateFormat.format(d);
                        Date date1=new SimpleDateFormat("yyyy-mm-dd").parse(min);
                        Date date2 = new SimpleDateFormat("yyyy-mm-dd").parse(strDate);
                        //System.out.println(date1);
                        //System.out.println(date2);
                        if (date2.compareTo(date1) < 0)
                            throw new DBAppException("you entered date below minimum");
                        max = Data.get(j)[Data.get(j).length - 1];
                        Date datemax=new SimpleDateFormat("yyyy-mm-dd").parse(max);
                        if (date2.compareTo(datemax) > 0)
                            throw new DBAppException("you entered date above maximum");
                    }
                    else {
                        if (colNameValue.get(colName).toString().compareTo(min) < 0) {
                            throw new DBAppException("you entered value below minimum");
                        }
                        max = Data.get(j)[Data.get(j).length - 1];
                        if (colNameValue.get(colName).toString().compareTo(max) > 1) {
                            throw new DBAppException("you entered value above maximum");
                        }
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
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+pageName+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(p);

            out.close();
            file.close();

            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println("IOException is caught");
        }

    }


    public static Page deserialize(String filePath){
        Page page = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            page = (Page) in.readObject();
            in.close();
            file.close();

            //      System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+filePath+"in deserialize page");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage() +"in deserialize");
        }
        return page;
    }


    public int readConfig(String key) throws IOException {
        int nRows=0;
        FileReader reader=new FileReader("src/main/resources/DBApp.config");

        Properties p=new Properties();
        p.load(reader);
        return Integer.parseInt(p.getProperty(key));
    }
    public static void serializeTable(Table table){
        String tableName = table.tableName;
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+tableName+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(table);

            out.close();
            file.close();
            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+ "in serialize table");
        }

    }
    /*public static Table deserializeTable(String tableName){
        Table table = null;
        String filePath = "src/main/resources/"+tableName+".bin";
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            table = (Table) in.readObject();
            in.close();
            file.close();

            System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        return table;
    }*/
    public Page getPage(String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Page p = null;
        //table doesnt exist
        Vector<String> tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        if(!tableNames.contains(tableName))
            throw new DBAppException("table doesn't exist!");
        else {
            serializetableNames(tableNames);
            String filePath = "src/main/resources/data/"+tableName+".bin";
            Table t = (Table) DeserializeTable(filePath);
            String clustering = t.clusteringKey;
            //I have the primary key and can search with it
            if(colNameValue.get(clustering)!=null){
                int x =   Collections.binarySearch(t.min,colNameValue.get(clustering).toString());
                //the pk is the min at that page
                if(x>0){


                    p = deserialize("src/main/resources/data/"+tableName+x+".bin");
                }
                else{
                    int c = x-1;

                    p=deserialize("src/main/resources/data/"+tableName+c+".bin");

                }
            }
            else{
                //looping over all table pages
                for(int i =0 ; i<t.pagesPath.size();i++){
                    String pageID =t.tableName+i;
                    String filePath1 = "src/main/resources/data/"+pageID+".bin";
                    p = deserialize(filePath1);
                    //looping over each page
                    for(int j =0;j<p.list.size();j++){
                        String currRow = p.list.get(j).toString();
                        String entry = colNameValue.toString();
                        if(currRow.contains(entry)) {
                            return p;

                        }
                    }
                }
            }
        }

        return p;}
    public int getRow (Page p , Hashtable<String,Object> colNameValue,String clusteringKey) throws DBAppException {
        int idx=0;
        //table doesnt exist
        //I have the primary key and can search with it
        if(colNameValue.get(clusteringKey)!=null){
            //getting the index by the clustering key
            idx = Collections.binarySearch(p.clusterings,colNameValue.get(clusteringKey).toString());
            if(idx>0)
                return idx;
            else if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                int x = Collections.binarySearch(p.overflowPage.clusterings, colNameValue.get(clusteringKey).toString());
                if(x>0){
                    return x;
                }
                else
                    throw new DBAppException("The entered row doesn't exist");

            }
            else
                throw new DBAppException("The entered row doesn't exist");
        }
        else{

            for(int i =0; i<p.list.size();i++){
                String currRow = p.list.get(i).toString();
                String entry = colNameValue.toString();
                if(currRow.contains(entry))
                    return i;
            }
            if(p.overflowPage!=null &&p.overflowPage.list.size()!=0){
                for(int i =0; i<p.overflowPage.list.size();i++){
                    String currRow = p.overflowPage.list.get(i).toString();
                    String entry = colNameValue.toString();
                    if(currRow.contains(entry))
                        return i;
                }
            }

        }


        return idx;}



    public static Table DeserializeTable(String path){
        Table table = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            table = (Table) in.readObject();
            in.close();
            file.close();

            //  System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"in deserialize table");
        }
        return table;


    }
    public  void serializeTableFunction(Table t){
        try
        {
            Vector<String> tableList = deserializeVectorS("src/main/resources/data/tablesList.bin");
            int vectorSize=tableList.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/"+t.tableName+vectorSize+".bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(t);

            out.close();
            file.close();

            //    System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"in serialize table functin");
        }



    }
    public static void serializetableList(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tablesList.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //   System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table list");
        }

    }

    public static Vector<String> deserializeVector(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            //    System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+"deserialize vector");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"deserialize vectoe");
        }
        return v;


    }
    public static void serializeindicies(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/indicies.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //  System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table names");
        }

    }
    public static void serializetableNames(Vector<String> v ){
        try
        {
            int vectorSize=v.size();
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream("src/main/resources/data/tableNames.bin");
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(v);

            out.close();
            file.close();

            //  System.out.println("Object has been serialized");

        }

        catch(IOException ex)
        {
            System.out.println(ex.getMessage()+"serialize table names");
        }

    }
    public static Vector<String> deserializeVectorS(String filePath){
        Vector<String> v = new Vector<>();
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(file);
            // Method for deserialization of object
            v = (Vector<String>) in.readObject();
            in.close();
            file.close();

            //     System.out.println("Object has been deserialized ");
        } catch (IOException ex) {
            System.out.println(ex.getMessage()+"'deserialize vectors'");
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage()+"'deserialize vectors'");
        }
        return v;


    }
    public void colNotFound (String tableName , Hashtable<String,Object> colNameValue) throws DBAppException {
        Vector<String[]> Data =new Vector<String[]>();
        String line = "";
        String splitBy = ",";
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
        HashSet<String> colNames = new HashSet<>();
        for(int i =0 ; i<Data.size();i++){
            if(Data.get(i)[0].equals(tableName)){
                colNames.add(Data.get(i)[1]);
            }
        }
        Set hash_set = colNameValue.keySet();

        if(!colNames.containsAll(hash_set)){
            throw new DBAppException("Columns entered not found in the table!");
        }
    }





    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void  buildArray (int level, Vector array){
        if (level==1)
            return;
        else {
            int newLevel =--level;
            for (int i=0;i<10;i++)
                array.add(new Vector(10));
            for(int i =0;i<10;i++)
                buildArray(newLevel,(Vector) array.get(i));
        }

    }

    public static void updateMetadata (String[] attributes,String tableName) {
        Vector<String[]> Data = new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i = 0;
        try {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array = line.split(splitBy);
                Data.add(array);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int k = 0; k < attributes.length; k++) {
            for (int j = 0; j < Data.size(); j++) {
                if (Data.get(j)[0].equals(tableName) && Data.get(j)[1].equals(attributes[k])) {
                   Data.get(j)[4]="True";

                }
            }
        }
        String result ="Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max"+'\n';
        for (int m=0;m<Data.size();m++){
            for (int n =0;n<Data.get(m).length;n++){
                if(n==Data.get(m).length-1)
                result+=Data.get(m)[n];
                else
                    result+=Data.get(m)[n]+",";

            }
            result+='\n';
        }
        try {
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv");
            csvWriter.write(result);
            csvWriter.flush();
            csvWriter.close();
        } catch(Exception e){
            System.out.print(e.getMessage());
        }




    }


    public static void createRanges (Index index){
        Vector<String[]> Data = new Vector<String[]>();
        String line = "";
        String splitBy = ",";
        int i = 0;
        try {

            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] array = line.split(splitBy);
                Data.add(array);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String min ="";
        String max ="";
        String type="";
        for (int k = 0; k < index.colNames.length; k++) {
            for (int j = 0; j < Data.size(); j++) {
                if (Data.get(j)[0].equals(index.tableName) && Data.get(j)[1].equals(index.colNames[k])) {
                    type=Data.get(j)[2];
                    min =Data.get(j)[5];
                    max=Data.get(j)[6];
                    createRangeList(index,index.colNames[k],type,min,max);

                }
            }
        }


    }


    public static void createRangeList (Index index ,String colName, String type, String min,String max){
        if (type.equals("java.lang.Integer")){
            int range = (int)Math.ceil(((Integer.parseInt(max)-Integer.parseInt(min))+1)/10.0);
           Vector r =  index.ranges.get(colName);
           r.add(min);
           for(int i=1;i<10;i++){
               int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
               int currMin = prevMin+range;
                if(currMin>Integer.parseInt(max)){
                    r.add(max);
                }
                else
               r.add(String.valueOf(currMin));
           }}
           else if(type.equals("java.lang.Double")){
            double range = Math.ceil(((Double.parseDouble(max)-Double.parseDouble(min))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add(min);
            for(int i=1;i<10;i++){
                double prevMin =Double.parseDouble((String.valueOf(r.get(i-1))));
                double currMin = prevMin+range;
                if(currMin>Integer.parseInt(max)){
                    r.add(max);
                }
                else
                r.add(String.valueOf(currMin));
            }}

         else if (type.equals("java.lang.String")){
            int range = (int)Math.ceil(((max.toLowerCase(Locale.ROOT).charAt(0))-(min.toLowerCase(Locale.ROOT).charAt(0))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add((int)min.toLowerCase(Locale.ROOT).charAt(0));
            for(int i=1;i<10;i++){
                int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
                int currMin = prevMin+range;
                if(currMin>max.toLowerCase(Locale.ROOT).charAt(0))
                    r.add((int)max.toLowerCase(Locale.ROOT).charAt(0));
                else
                r.add(String.valueOf(currMin));
            }}
    else  if (type.equals("java.util.Date")){
            String minDate = min.replace("-","");
            String maxDate = max.replace("-","");
            int range = (int)Math.ceil(((Integer.parseInt(maxDate)-Integer.parseInt(minDate))+1)/10.0);
            Vector r =  index.ranges.get(colName);
            r.add(minDate);
            for(int i=1;i<10;i++){
                int prevMin =Integer.parseInt(String.valueOf(r.get(i-1)));
                int currMin = prevMin+range;
                if(currMin>Integer.parseInt(maxDate))
                    r.add(String.valueOf(maxDate));
                r.add(String.valueOf(currMin));
            }
        }
    }



    public static void insertIntoBucketUpdate(String pagePath, Vector colValues , Index index,int currentDimension,Vector data  ){
            String dimensionName = index.colNames[currentDimension];
            Vector currentRanges = index.ranges.get(dimensionName);
            Object currentValue = colValues.get(currentDimension);
            //BASE CASE >>>  INSERTING INTO BUCKET
            if (currentDimension== index.colNames.length-1){
                String bucket= null;
                for (int i=0;i<currentRanges.size();i++) {
                    if (i == currentRanges.size() - 1) {
                        bucket = (String) data.get(currentRanges.size() - 1);
                        break;
                    } else {
                            String type=getType(dimensionName);
                            if(type.equals("java.lang.String")){
                                Character c = currentValue.toString().charAt(0);
                                Character range = (Character) currentRanges.get(i);
                                if (c<=range) {
                                    bucket= (String)data.get(i);
                                    break;
                                }
                            }
                            else if(type.equals("java.lang.Integer")){
                                if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                    bucket= (String)data.get(i);
                                    break;
                                }
                            }
                            else if(type.equals("java.lang.Double")){
                                if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                    bucket= (String)data.get(i);
                                    break;
                                }
                            }
                            else if(type.equals("java.util.Date")){
                                String currDate = ((Date) currentValue).toString();
                                int numcurDate = Integer.parseInt(currDate);
                                int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                                if (numcurDate<=rangeDate) {
                                    bucket= (String)data.get(i);
                                    break;
                                }
                            }
                            else
                            {
                                try {
                                    throw new DBAppException("WRONG DATATYPE");
                                } catch (DBAppException e) {
                                    System.out.println(e.getMessage());
                                }
                            }



                        }
                }}}
                /*if(bucket== null)
                    bucket = new Bucket("",index);
                else
                    bucket = Bucket.DeserializeBucket(bucketpath);
                if(contains(index.colNames,index.clusteringKey)){
                    Vector a = bucket.list.get(colnameval.get(index.clusteringKey));
                    if(a== null){
                        if(bucket.noOfEntries<bucket.max) {
                            bucket.noOfEntries++;
                            a = new Vector<String>();
                        }
                        else{
                            a= new Vector<String>();
                            a.add(pagePath);
                            if(bucket.overFlow==null) {
                                bucket.overFlow = new Bucket(bucket.BucketId + "Over", index);
                                bucket.overFlow.list.put((String) colnameval.get(index.clusteringKey), a);
                            }
                            else{
                                a.add(pagePath);
                                bucket.list.put((String) colnameval.get(index.clusteringKey), a);
                            }
                        }

                    }
                    a.add(pagePath);
                    bucket.list.put((String) colnameval.get(index.clusteringKey), a);

                }
                //use left most col if primary key doesn't exist
                else{
                    Vector a = bucket.list.get(colnameval.get(0));
                    if(a== null){
                        if(bucket.noOfEntries<bucket.max) {
                            a = new Vector<String>();
                            bucket.noOfEntries++;
                        }
                        else{
                            a= new Vector<String>();
                            a.add(pagePath);
                            if(bucket.overFlow==null) {
                                bucket.overFlow = new Bucket(bucket.BucketId + "Over", index);
                                bucket.overFlow.list.put((String) colnameval.get(index.clusteringKey), a);
                            }
                            else{
                                a.add(pagePath);
                                bucket.list.put((String) colnameval.get(index.clusteringKey), a);
                            }
                        }

                    }
                    a.add(pagePath);
                    bucket.list.put((String) colnameval.get(index.clusteringKey), a);
                }


            }
            else{
                currentDimension++;
                insertIntoBucket ( pagePath,indexes ,index,currentDimension, (Vector) data.get(indexes[currentDimension]), colnameval );

            }
        bucket.serializeBucket();
                // deserializing bucket


                //inserting record into it


                //serializng it



                return;
            } else {
                for (int i=0;i<currentRanges.size();i++){
                    if (i==currentRanges.size()-1)
                       insertIntoBucketUpdate(pagePath,colValues,index,++currentDimension,(Vector) data.get(currentRanges.size()-1));
                    else{
                        String type=getType(dimensionName);
                        if(type.equals("java.lang.String")){
                            Character c = currentValue.toString().charAt(0);
                            Character range = (Character) currentRanges.get(i);
                            if (c<=range) {
                                insertIntoBucketUpdate(pagePath, colValues, index, ++currentDimension, (Vector) data.get(i));
                                break;
                            }
                        }
                        else if(type.equals("java.lang.Integer")){
                            if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                insertIntoBucketUpdate(pagePath, colValues, index, ++currentDimension, (Vector) data.get(i));
                                break;
                            }
                        }
                        else if(type.equals("java.lang.Double")){
                            if ((currentValue.toString()).compareTo(currentRanges.get(i).toString()) < 0) {
                                insertIntoBucketUpdate(pagePath, colValues, index, ++currentDimension, (Vector) data.get(i));
                                break;
                            }
                        }
                        else if(type.equals("java.util.Date")){
                            String currDate = ((Date) currentValue).toString();
                            int numcurDate = Integer.parseInt(currDate);
                            int rangeDate = Integer.parseInt(currentRanges.get(i).toString());
                            if (numcurDate<=rangeDate) {
                                insertIntoBucketUpdate(pagePath, colValues, index, ++currentDimension, (Vector) data.get(i));
                                break;
                            }
                        }
                        else
                        {
                            try {
                                throw new DBAppException("WRONG DATATYPE");
                            } catch (DBAppException e) {
                                System.out.println(e.getMessage());
                            }
                        }

                        if(currentValue.toString().compareTo((String) currentRanges.get(i))>0 &&currentValue.toString().compareTo((String) currentRanges.get(i+1))<0 ) {
                            insertIntoBucketUpdate(pagePath, colValues, index, ++currentDimension, (Vector) data.get(i));
                            return;
                        }
                    }


                }




            }


    }*/
   /* public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        Index index = new Index(tableName,columnNames);

        loopPages(tableName,columnNames,index);

    }*/
    public static void updateIndex(String pagePath , Hashtable<String,Object>colnamevalue , String tableName){
        Table target = DeserializeTable("src/main/resources/data/"+tableName+".bin");
        for(int i =0 ;i<target.indicies.size();i++ ){
            Index index = Index.DeserializeIndex(target.indicies.get(i));
            Vector colValues = new Vector<String>();
            for(int j  =0 ; j<index.colNames.length;j++){
                colValues.add(colnamevalue.get(index.colNames[i]));
                insertIntoBucketUpdate (pagePath,colValues , index,0,index.grid);
            }
        }

    }
    public static void insertIntoBucket (String pagePath, int[] indexes , Index index,int currentDimension,Vector data , Hashtable<String , Object> colnameval ) {
        Bucket bucket =null;
        if(currentDimension== index.colNames.length-1){
            String bucketpath = (String)data.get(indexes[indexes.length-1]);
            if(bucketpath == null)
                 bucket = new Bucket(bucketpath,index);
            else
             bucket = Bucket.DeserializeBucket(bucketpath);
            if(contains(index.colNames,index.clusteringKey)){
                Vector a = bucket.list.get(colnameval.get(index.clusteringKey));
                if(a== null){
                    if(bucket.noOfEntries<bucket.max) {
                        bucket.noOfEntries++;
                        a = new Vector<String>();
                    }
                    else{
                        a= new Vector<String>();
                        a.add(pagePath);
                        if(bucket.overFlow==null) {
                            bucket.overFlow = new Bucket(bucket.BucketId + "Over", index);
                            bucket.overFlow.list.put((String) colnameval.get(index.clusteringKey), a);
                        }
                        else{
                            a.add(pagePath);
                            bucket.list.put((String) colnameval.get(index.clusteringKey), a);
                        }
                    }

                }
                a.add(pagePath);
                bucket.list.put((String) colnameval.get(index.clusteringKey), a);

            }
            //use left most col if primary key doesn't exist
            else{
                Vector a = bucket.list.get(colnameval.get(0));
                if(a== null){
                    if(bucket.noOfEntries<bucket.max) {
                        a = new Vector<String>();
                        bucket.noOfEntries++;
                    }
                    else{
                        a= new Vector<String>();
                        a.add(pagePath);
                        if(bucket.overFlow==null) {
                            bucket.overFlow = new Bucket(bucket.BucketId + "Over", index);
                            bucket.overFlow.list.put((String) colnameval.get(index.clusteringKey), a);
                        }
                        else{
                            a.add(pagePath);
                            bucket.list.put((String) colnameval.get(index.clusteringKey), a);
                        }
                    }

                }
                a.add(pagePath);
                bucket.list.put((String) colnameval.get(index.clusteringKey), a);
            }


        }
        else{
            currentDimension++;
            insertIntoBucket ( pagePath,indexes ,index,currentDimension, (Vector) data.get(indexes[currentDimension]), colnameval );

            }
        bucket.serializeBucket();
    }
public static boolean contains (String [] arr, String s ){
        for(int i =0 ;i<arr.length;i++){
            if(arr[i].equals(s))
                return true;
        }
        return false;
}
        public static void loopPages(String tableName, String[] columnNames,Index index){
        Table table =DeserializeTable("src/main/resources/data/" + tableName + ".bin");
        for(int i=0;i<table.pagesPath.size();i++){
            loopPage(tableName, table.pagesPath.get(i), columnNames, index);
            Page page=deserialize(table.pagesPath.get(i));
            Page overflow=new Page("name");
            if (page.overflowPage != null) {

                overflow = deserialize("src/main/resources/data" + "/" + tableName + i + "Over" + ".bin");
                String p="src/main/resources/data" + "/" + tableName + i + "Over" + ".bin";  // overflow page path
                loopPage(tableName, p, columnNames, index);
            }
            serializePage(overflow);
            serializePage(page);
        }
        serializeTable(table);
    }
    public static void  loopPage(String tableName, String filepath, String[] columnNames,Index index)  {
        Page page=deserialize(filepath);
        for(int j=0;j<page.clusterings.size();j++){
            // Hashtable<String,Integer> indexes=new Hashtable<String,Integer>();
            int [] indexes =new int [columnNames.length];
            Hashtable row=page.list.get(j);
            for(int i=0;i<columnNames.length;i++){
                Object value =row.get(columnNames[i]);
                Vector ranges=index.ranges.get(columnNames[i]);// the vector of MIN values  of columnNames[i]
                for(int g=0;g<ranges.size();g++){       // to know which index of the cell we should insert in
                    String type=getType(value);
                    if(type.equals("java.lang.String")){
                        Character c = value.toString().charAt(0);
                        Character range = (Character) ranges.get(g);
                        if (c<=range) {
                            indexes[i]=g-1;
                        }
                    }
                    else if(type.equals("java.lang.Integer")){
                        if (((Integer)value).compareTo((Integer)ranges.get(g)) < 0) {
                            indexes[i]=g-1;
                        }
                    }
                    else if(type.equals("java.lang.Double")){
                        if (((Double)value).compareTo((Double)ranges.get(g)) < 0) {
                            indexes[i]=g-1;
                        }
                    }
                    else if(type.equals("java.util.Date")){
                        String currDate = ((Date) value).toString();
                        int numcurDate = Integer.parseInt(currDate);
                        int rangeDate = Integer.parseInt(ranges.get(g).toString());
                        if (numcurDate<=rangeDate) {
                            indexes[i]=g-1;
                        }
                    }
                    else
                    {
                        try {
                            throw new DBAppException("WRONG DATATYPE");
                        } catch (DBAppException e) {
                            System.out.println(e.getMessage());
                        }
                    }



                }

            }
            //public static void insertIntoBucket (String pagePath, int[] indexes , Index index,int currentDimension,Vector data , Hashtable<String , Object> colnameval ) {

                insertIntoBucket(filepath,indexes,index,0,index.grid,row);
           // KareemMethod(j,filepath,indexes,index);// j is the index of the row inside the page
            //filepath is the path of the page containing the row
            //indexes is the array  that contains the tuple that has the indexes
            // that the row should be inserted in
            //index is the Index you should insert the path in

        }

        serializePage(page);
    }













    public static void main (String[] args) throws DBAppException, IOException, ParseException {
        String [] minDate = "22-7-1999".split("-");
        String [] maxDate = "30-5-2014".split("-");
        //    public static void createRangeList (Index index ,String colName, String type, String min,String max){
        Index i = new Index("student", new String[]{"name"});
        createRangeList(i,"name","java.util.Date","1999-11-01","2012-11-01");
           System.out.println(i.ranges.get("name").toString());
        //System.out.println(Integer.parseInt("12344"));
       /*FileWriter csvWriter = null;
        Table t = DeserializeTable("src/main/resources/data/pcs.bin");
        //System.out.println(readConfig("MaximumRowsCountinPage"));
        Page p = deserialize("src/main/resources/data/students1.bin");
       System.out.println(t.min.toString());
       System.out.println(t.max.toString());
        for(int i =0;i<p.list.size();i++){
            try {
                csvWriter = new FileWriter("src/main/resources/t.csv" ,true);

            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                csvWriter.write(p.list.get(i).toString());
                csvWriter.write('\n');
                csvWriter.flush();
                csvWriter.close();
            }
            catch (IOException ex){
                System.out.println(ex.getMessage());}
        }
        System.out.println(t.max.get(0));*/


    }}
