package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

public class LoadService implements RequestHandler<Request, HashMap<String, Object>> {

    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data.
        Inspector inspector = new Inspector();
         inspector.inspectAll();
        LambdaLogger logger = context.getLogger();

        // ****************START FUNCTION IMPLEMENTATION*************************

        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        String sql = " INSERT INTO SALES_RECORD VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
//        S3Object s3Object = null;

        try {
            // ******************get database connection******************************

            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);
            PreparedStatement ps = con.prepareStatement("SHOW TABLES WHERE TABLES_IN_TEST='SALES_RECORD'");
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                // table does not exist, and should be created
                logger.log("trying to create table");
                ps = con.prepareStatement(
                        "CREATE TABLE SALES_RECORD (" +
                                " `region` VARCHAR(255)," +
                                " `country` VARCHAR(255)," +
                                " `item_type` VARCHAR(255)," +
                                " `sales_channel` VARCHAR(255)," +
                                " `order_priority` VARCHAR(255)," +
                                " `order_date` DATE," +
                                " `order_id` INT NOT NULL," +
                                " `ship_date` DATE," +
                                " `units_sold` INT," +
                                " `unit_price` DOUBLE," +
                                " `unit_cost` DOUBLE," +
                                " `total_revenue` DOUBLE," +
                                " `total_cost` DOUBLE," +
                                " `total_profit` DOUBLE," +
                                " `order_processing_time` INT," +
                                " `gross_margin` DOUBLE," +
                                " PRIMARY KEY (`order_id`));");
                ps.execute();
                ps.close();
            }
            rs.close();
            // file******************************************
            logger.log("Before creating S3 client");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();

            logger.log("Bucketname : " + bucketname + "\n");
            logger.log("Filename : " + filename + "\n");

            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename)); // get object file using
            InputStream objectData = s3Object.getObjectContent(); // get content of the file



            String line;
            Scanner scanner = new Scanner(objectData); // scanning data line by line
            // Skipped header line
            scanner.nextLine();
            while (scanner.hasNext()) {
                line = scanner.nextLine();

                if (line != null) {

                    String[] array = line.split(",");
                    ps = con.prepareStatement(sql);

                    ps.setString(1, array[0]);
                    ps.setString(2, array[1]);
                    ps.setString(3, array[2]);
                    ps.setString(4, array[3]);
                    ps.setString(5, array[4]);
                    ps.setDate(6, getDate(array[5]));
                    ps.setInt(7, getInt(array[6]));
                    ps.setDate(8, getDate(array[7]));
                    ps.setInt(9, getInt(array[8]));
                    ps.setDouble(10, getDouble(array[9]));
                    ps.setDouble(11, getDouble(array[10]));
                    ps.setDouble(12, getDouble(array[11]));
                    ps.setDouble(13, getDouble(array[12]));
                    ps.setDouble(14, getDouble(array[13]));
                    ps.setInt(15, getInt(array[14]));
                    ps.setDouble(16, getDouble(array[15]));

                    ps.executeUpdate();
                    ps.close();
                }
            }
            scanner.close();
        } catch (Exception e) {
            /// logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
            e.printStackTrace();
        }
//        } finally {
//            if (s3Object != null) {
//                try {
//                    s3Object.close();
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
        Response r = new Response();
        r.setValue("Bucket:" + bucketname + " filename:" + filename);
        inspector.consumeResponse(r);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private Integer getInt(String integer) {
        return Integer.parseInt(integer);
    }

    private Double getDouble(String doubleVal) {
        return Double.parseDouble(doubleVal);
    }

    private Date getDate(String date) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        return new java.sql.Date(formatter.parse(date).getTime());
    }

}
