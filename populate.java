/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Shweta
 */
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.json.simple.JSONArray;

public class populate {

    public void populateUser(String FileName) throws IOException {
        System.out.println("Inerting User Data");
        JSONParser parser = new JSONParser();
        populate p = new populate();
        Connection con = null;
        java.sql.Date sqlDate;
        FileInputStream fstream = null;
        BufferedReader br = null;

        try {
            //FileInputStream fstream = new FileInputStream("C:\\Shweta\\Database\\Assignments\\HW3\\Test_yelp_dataSheet\\yelp_user.json");
            fstream = new FileInputStream(FileName);
            br = new BufferedReader(new InputStreamReader(fstream));
            Object obj;
            String strLine;
            con = p.openConnection();
            Statement stmt = con.createStatement();

            while ((strLine = br.readLine()) != null) {

                obj = parser.parse(strLine);

                JSONObject jsonObject = (JSONObject) obj;

                String memberSince = (String) jsonObject.get("yelping_since");
                /*int year = Integer.parseInt(memberSince.split("-")[0]);
                int month = Integer.parseInt(memberSince.split("-")[1]);
                int day = 1;
                String date = year + "/" + month + "/" + day;
                java.util.Date myDate = new java.util.Date(date);
                sqlDate = new java.sql.Date(myDate.getTime());*/

                Long reviewCount = (Long) jsonObject.get("review_count");

                String user_id = (String) jsonObject.get("user_id");
                String name = (String) jsonObject.get("name");
                Long fans = (Long) jsonObject.get("fans");
                String type = (String) jsonObject.get("type");
                Double average_stars = (Double) jsonObject.get("average_stars");
                JSONArray friends = (JSONArray) jsonObject.get("friends");
                int numberOfFriend = friends.size();

                String insertTableSQL = "INSERT INTO YELP_USER"
                        + "(USER_ID, NAME, YELPING_SINCE, REVIEW_COUNT, FANS, AVERAGE_STARS, TYPE, NUMBER_OF_FRIENDS)"
                        + "VALUES(?,?,?,?,?,?,?,?)";

                PreparedStatement preparedStmt = con.prepareStatement(insertTableSQL);
                preparedStmt.setString(1, user_id.trim());
                preparedStmt.setString(2, name.trim());
                //preparedStmt.setDate(3, sqlDate);
                preparedStmt.setString(3, memberSince);
                preparedStmt.setLong(4, reviewCount);
                preparedStmt.setLong(5, fans);
                preparedStmt.setDouble(6, average_stars);
                preparedStmt.setString(7, type);
                preparedStmt.setInt(8, numberOfFriend);

                preparedStmt.execute();
                preparedStmt.close();

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Errors occurs when communicating with the database server: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the database driver");
        } finally {
            // Never forget to close database connection 
            p.closeConnection(con);
            br.close();
            fstream.close();
        }

        System.out.println("YELP_USER data INSERTED");
    }

    public void populateBusiness(String FileName) throws IOException {
        System.out.println("INSERTING BUSINESS Data");
        JSONParser parser = new JSONParser();
        populate p = new populate();
        Connection con = null;
        FileInputStream fstream = null;
        BufferedReader br = null;
        ArrayList<String> definedCategories = new ArrayList<String>();

        definedCategories.add("Active Life");
        definedCategories.add("Arts & Entertainment");
        definedCategories.add("Automotive");
        definedCategories.add("Car Rental");
        definedCategories.add("Cafes");
        definedCategories.add("Beauty & Spas");
        definedCategories.add("Convenience Stores");
        definedCategories.add("Dentists");
        definedCategories.add("Doctors");
        definedCategories.add("Drugstores");
        definedCategories.add("Department Stores");
        definedCategories.add("Education");
        definedCategories.add("Event Planning & Services");
        definedCategories.add("Flowers & Gifts");
        definedCategories.add("Food");
        definedCategories.add("Health & Medical");
        definedCategories.add("Home Services");
        definedCategories.add("Home & Garden");
        definedCategories.add("Hospitals");
        definedCategories.add("Hotels & Travel");
        definedCategories.add("Hardware Stores");
        definedCategories.add("Grocery");
        definedCategories.add("Medical Centers");
        definedCategories.add("Nurseries & Gardening");
        definedCategories.add("Nightlife");
        definedCategories.add("Restaurants");
        definedCategories.add("Shopping");
        definedCategories.add("Transportation");

        try {
            //FileInputStream fstream = new FileInputStream("C:\\Shweta\\Database\\Assignments\\HW3\\Test_yelp_dataSheet\\yelp_business.json");
            fstream = new FileInputStream(FileName);
            br = new BufferedReader(new InputStreamReader(fstream));
            Object obj;
            String strLine;
            con = p.openConnection();
            //  Statement stmt = con.createStatement();
            int count = 1;
            while ((strLine = br.readLine()) != null) {

                ArrayList<String> mainCategory = new ArrayList<String>();
                ArrayList<String> subCategory = new ArrayList<String>();

                obj = parser.parse(strLine);

                JSONObject jsonObject = (JSONObject) obj;

                String business_id = (String) jsonObject.get("business_id");
                String name = (String) jsonObject.get("name");
                String type = (String) jsonObject.get("type");
                String full_address = (String) jsonObject.get("full_address");
                String city = (String) jsonObject.get("city");
                String state = (String) jsonObject.get("state");
                Long review_count = (Long) jsonObject.get("review_count");
                Double stars = (Double) jsonObject.get("stars");

                String insertTableSQL = "INSERT INTO BUSINESS"
                        + "(business_id, name, type, full_address, city, state, review_count, star)"
                        + "VALUES(?,?,?,?,?,?,?,?)";

                PreparedStatement preparedStmt = con.prepareStatement(insertTableSQL);
                preparedStmt.setString(1, business_id.trim());
                preparedStmt.setString(2, name.trim());
                preparedStmt.setString(3, type);
                preparedStmt.setString(4, full_address.trim());
                preparedStmt.setString(5, city.trim());
                preparedStmt.setString(6, state.trim());
                preparedStmt.setLong(7, review_count);
                preparedStmt.setDouble(8, stars);

                preparedStmt.execute();

                preparedStmt.close();

                JSONArray categories = (JSONArray) jsonObject.get("categories");

                Iterator<String> iterator = categories.iterator();
                while (iterator.hasNext()) {

                    String cat = iterator.next();
                    //  System.out.println("Category:"+cat);
                    /* check if iterator.hasNext() is main category
                    if yes, add the value in mainCategory ArrayList otherwise add it in subCategory Arraylist*/
                    if (definedCategories.indexOf(cat) != -1) {
                        mainCategory.add(cat);
                    } else {
                        subCategory.add(cat);
                    }

                }

                for (String str : mainCategory) {
                    //System.out.println("Business ID:" + business_id + "-->MainCategory: " + str);

                    if (subCategory.isEmpty()) {
                        String insertTableMainCategory = "INSERT INTO category"
                                + "(category_id, business_id, maincategory_name,subcategory_name)"
                                + "VALUES(?,?,?,?)";
                        PreparedStatement preparedStmt1 = con.prepareStatement(insertTableMainCategory);
                        preparedStmt1.setInt(1, count);
                        preparedStmt1.setString(2, business_id.trim());
                        preparedStmt1.setString(3, str.trim());
                        preparedStmt1.setString(4, null);
                        preparedStmt1.execute();
                        preparedStmt1.close();
                        count++;
                    } else {
                        for (String str2 : subCategory) {
                            String insertTableSubCategory = "INSERT INTO category"
                                    + "(category_id, business_id, maincategory_name, subcategory_name)"
                                    + "VALUES(?,?,?,?)";

                            PreparedStatement preparedStmt2 = con.prepareStatement(insertTableSubCategory);
                            preparedStmt2.setInt(1, count);
                            preparedStmt2.setString(2, business_id.trim());
                            preparedStmt2.setString(3, str.trim());
                            preparedStmt2.setString(4, str2);
                            preparedStmt2.execute();
                            preparedStmt2.close();
                            // execute insert SQL stetement
                            count++;

                        }
                    }

                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Errors occurs when communicating with the database server: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the database driver");
        } finally {
            // Never forget to close database connection 
            p.closeConnection(con);
            br.close();
            fstream.close();
        }
        System.out.println("BUSINESS data INSERTED");
    }

    public void populateCheckin(String FileName) throws IOException {
        System.out.println("CHECKIN Data Inserting");
        //System.out.println("In Populate CheckIn " + FileName);
        JSONParser parser = new JSONParser();
        populate p = new populate();
        Connection con = null;
        FileInputStream fstream = null;
        BufferedReader br = null;

        try {
            //FileInputStream fstream = new FileInputStream("C:\\Shweta\\Database\\Assignments\\HW3\\Test_yelp_dataSheet\\yelp_checkin.json");
            fstream = new FileInputStream(FileName);
            br = new BufferedReader(new InputStreamReader(fstream));
           // System.out.println("Readin File");
            Object obj;
            String strLine;
            con = p.openConnection();
            Statement stmt = con.createStatement();

            while ((strLine = br.readLine()) != null) {
               // System.out.println(strLine);
                obj = parser.parse(strLine);

                JSONObject jsonObject = (JSONObject) obj;
                String business_id = (String) jsonObject.get("business_id");
                JSONObject checkIns = (JSONObject) jsonObject.get("checkin_info");

                String keyStr = null;

                for (Object key : checkIns.keySet()) {
                    //based on you key types
                    keyStr = (String) key;

                    Long numberOfCheckin = (Long) checkIns.get(keyStr);

                    String[] parts = keyStr.split("-");
                    Long startHour = Long.parseLong(parts[0]);
                    Long day_id = Long.parseLong(parts[1]);

                    String insertCheckin = "INSERT INTO Checkin"
                            + "(business_id, startHour, day_id, numberOfCheckin)"
                            + "VALUES(?,?,?,?)";

                    PreparedStatement prepareStatement = con.prepareStatement(insertCheckin);

                   // System.out.println("Data: "+business_id+"--"+startHour+"--"+day_id+"--"+numberOfCheckin);
                    prepareStatement.setString(1, business_id);
                    prepareStatement.setLong(2, startHour);
                    prepareStatement.setLong(3, day_id);
                    prepareStatement.setLong(4, numberOfCheckin);

                    prepareStatement.execute();
                  //  System.out.println("Inserted Data");
                    prepareStatement.close();

                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Errors occurs when communicating with the database server: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the database driver");
        } finally {
            // Never forget to close database connection 
            p.closeConnection(con);
            br.close();
            fstream.close();
        }
        System.out.println("CHECKIN data INSERTED");
    }

    public void populateReview(String FileName) throws IOException {
        System.out.println("REVIEW Data Inserting");
        //System.out.println("In Populate Review "+FileName);
        JSONParser parser = new JSONParser();
        populate p = new populate();
        Connection con = null;
        java.sql.Date sqlDate;
        FileInputStream fstream = null;
        BufferedReader br = null;

        try {
            //FileInputStream fstream = new FileInputStream("C:\\Shweta\\Database\\Assignments\\HW3\\Test_yelp_dataSheet\\yelp_review.json");
            fstream = new FileInputStream(FileName);
            br = new BufferedReader(new InputStreamReader(fstream));
            Object obj;
            String strLine;
            con = p.openConnection();
            Statement stmt = con.createStatement();

            while ((strLine = br.readLine()) != null) {

                obj = parser.parse(strLine);

                JSONObject jsonObject = (JSONObject) obj;

                /*
                 "text": "dr. goldberg offers everything i look for in a general practitioner,"
                 */
                String review_date = (String) jsonObject.get("date");
                int year = Integer.parseInt(review_date.split("-")[0]);
                int month = Integer.parseInt(review_date.split("-")[1]);
                int day = Integer.parseInt(review_date.split("-")[2]);
                String date = year + "/" + month + "/" + day;
                java.util.Date myDate = new java.util.Date(date);
                sqlDate = new java.sql.Date(myDate.getTime());

                String review_id = (String) jsonObject.get("review_id");
                String user_id = (String) jsonObject.get("user_id");
                String business_id = (String) jsonObject.get("business_id");
                Long star = (Long) jsonObject.get("stars");
                String reviewtext = (String) jsonObject.get("text");
                int length = (300 < reviewtext.length()) ? 300 : reviewtext.length();
                String text = reviewtext.substring(0, length);

                JSONObject votes = (JSONObject) jsonObject.get("votes");
                Long vote_funny = (Long) votes.get("funny");
                Long vote_useful = (Long) votes.get("useful");
                Long vote_cool = (Long) votes.get("cool");

                String insertTableSQL = "INSERT INTO REVIEW"
                        + "(review_id, user_id, business_id, star, VOTE_FUNNY, VOTE_USEFUL, VOTE_COOL, publish_date, text)"
                        + "VALUES(?,?,?,?,?,?,?,?,?)";

                PreparedStatement preparedStmt = con.prepareStatement(insertTableSQL);
                preparedStmt.setString(1, review_id.trim());
                preparedStmt.setString(2, user_id.trim());
                preparedStmt.setString(3, business_id.trim());
                preparedStmt.setLong(4, star);
                preparedStmt.setLong(5, vote_funny);
                preparedStmt.setLong(6, vote_useful);
                preparedStmt.setLong(7, vote_cool);
                preparedStmt.setDate(8, sqlDate);
                preparedStmt.setString(9, text.trim());

                preparedStmt.execute();
                preparedStmt.close();

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Errors occurs when communicating with the database server: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the database driver");
        } finally {
            // Never forget to close database connection 
            p.closeConnection(con);
            br.close();
            fstream.close();
        }
        System.out.println("REVIEW data INSERTED");
    }

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

        populate populate = new populate();
        Connection con = null;
        ArrayList<String> stringList = new ArrayList<String>(Arrays.asList(args));
        int i = 0;
        /*
        try {

            con = populate.openConnection();
            Statement stmt = con.createStatement();

            System.out.println("Deleting old Data");
            String deleteCheckin = "DELETE FROM Checkin";
            String deleteReview = "DELETE FROM Review";
            String deleteCategory = "DELETE FROM Category";
            String deleteBusiness = "DELETE FROM Business";
            String deleteUser = "DELETE FROM yelp_user";

            stmt.executeUpdate(deleteCheckin);
            stmt.executeUpdate(deleteReview);
            stmt.executeUpdate(deleteCategory);
            stmt.executeUpdate(deleteBusiness);
            stmt.executeUpdate(deleteUser);

            stmt.close();
            System.out.println("old Data Deleted");
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the database driver");
        } finally {
            // Never forget to close database connection 
            populate.closeConnection(con);
        }
         */
        System.out.println("Inserting New Data");
        for (i = 0; i < args.length; i++) {

            if (stringList.contains("yelp_user.json") && i == 0) {
                //System.out.println(args[stringList.indexOf("yelp_user.json")]);
               // populate.populateUser(args[stringList.indexOf("yelp_user.json")]);

            } else if (stringList.contains("yelp_business.json") && i == 1) {
                //System.out.println(args[stringList.indexOf("yelp_business.json")]);
               // populate.populateBusiness(args[stringList.indexOf("yelp_business.json")]);

            } else if (stringList.contains("yelp_review.json") && i == 2) {
                //System.out.println(args[stringList.indexOf("yelp_review.json")]);
               // populate.populateReview(args[stringList.indexOf("yelp_review.json")]);

            } else if (stringList.contains("yelp_checkin.json") && i == 3) {
                System.out.println(args[stringList.indexOf("yelp_checkin.json")]);
                populate.populateCheckin(args[stringList.indexOf("yelp_checkin.json")]);

            }

        }
        System.out.println("All New Data Inserted");
    }

    private Connection openConnection() throws SQLException, ClassNotFoundException {
        // Load the Oracle database driver 
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());

        /* 
           Here is the information needed when connecting to a database 
           server. These values are now hard-coded in the program. In 
           general, they should be stored in some configuration file and 
           read at run time. 
         */
        String host = "localhost";
        String port = "1521";
        String dbName = "XE";
        String userName = "shweta";
        String password = "shweta";

        // Construct the JDBC URL 
        String dbURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
        return DriverManager.getConnection(dbURL, userName, password);
    }

    /**
     * Close the database connection
     *
     * @param con
     */
    private void closeConnection(Connection con) {
        try {
            con.close();
        } catch (SQLException e) {
            System.err.println("Cannot close connection: " + e.getMessage());
        }
    }
}
