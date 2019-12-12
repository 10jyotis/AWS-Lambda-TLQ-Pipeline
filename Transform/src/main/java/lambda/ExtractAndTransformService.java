package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Jyoti Shankar
 */
public class ExtractAndTransformService implements RequestHandler<Request, HashMap<String, Object>> {

    private static final int ORDER_PRIORITY_INDEX = 4;
    private static final int ORDER_DATE_INDEX = 5;
    private static final int ORDER_ID_INDEX = 6;
    private static final int SHIP_DATE_INDEX = 7;
    private static final int TOTAL_REVENUE_INDEX = 11;
    private static final int TOTAL_PROFIT_INDEX = 13;
    private static final String TRANSFORMED_CSV_FILE_NAME = "transformed-csv-template/transformed.csv";


    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        //Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        inspector.addAttribute("error", "");

        //****************process CSV************
        LambdaLogger logger = context.getLogger();
        String bucketName = request.getBucketname();
        String filename = request.getFilename();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line
        StringWriter sw = new StringWriter();
        Scanner scanner = new Scanner(objectData);
        sw.append(scanner.nextLine()).append(",Order Processing Time").append(",Gross Margin");
        Set<String> orderIds = new HashSet<>();
        try {
            while (scanner.hasNext()) {
                String[] strArray = scanner.nextLine().split(",");
                if (orderIds.contains(strArray[ORDER_ID_INDEX])) {
                    continue;
                }
                orderIds.add(strArray[ORDER_ID_INDEX]);
                sw.append("\n");
                transformAndAppendRow(strArray, sw);
            }
            scanner.close();

            byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
            sw.flush();
            sw.close();
            writeCSVInS3(s3Client, bucketName, bytes);
        } catch (Exception ex) {
            logger.log("Encountered unknown exception..");
        }

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket:" + bucketName + " input filename:" + filename + " transformed filename: "
                + TRANSFORMED_CSV_FILE_NAME);
        inspector.consumeResponse(response);
        logger.log("ExtractAndTransformService bucketname:" + bucketName + " input filename:" + filename
                + " transformed filename:" + TRANSFORMED_CSV_FILE_NAME);

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private void writeCSVInS3(AmazonS3 s3Client, String bucketName, byte[] bytes) {
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("plain/text");

        // Create new file on S3
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, TRANSFORMED_CSV_FILE_NAME, is, meta);
        s3Client.putObject(putObjectRequest);
    }

    private void transformAndAppendRow(String[] strArray, StringWriter writer) throws ParseException {
        for (int i = 0; i < strArray.length; i++) {
            if (i == ORDER_PRIORITY_INDEX) {
                // transform order priority
                writer.append(transformOrderPriority(strArray[i]));
            } else {
                writer.append(strArray[i]);
            }
            writer.append(",");
        }
        writer.append(Integer.toString(
                computeOrderProcessingTime(strArray[ORDER_DATE_INDEX], strArray[SHIP_DATE_INDEX])));
        writer.append(",");
        writer.append(Float.toString(
                computeGrossMargin(strArray[TOTAL_PROFIT_INDEX], strArray[TOTAL_REVENUE_INDEX])));
    }

    private int computeOrderProcessingTime(String orderDate, String shipDate) throws ParseException {
        if (StringUtils.isNullOrEmpty(orderDate) || StringUtils.isNullOrEmpty(shipDate)) {
            return 0;
        }
        return (int) ChronoUnit.DAYS.between(parseDate(orderDate).toInstant(), parseDate(shipDate).toInstant());
    }

    private String transformOrderPriority(String str) {
        switch (str) {
            case "L":
                return "Low";
            case "M":
                return "Medium";
            case "H":
                return "High";
            case "C":
                return "Critical";
            default:
                return null;
        }
    }

    private float computeGrossMargin(String totalProfit, String totalRevenue) {
        return Float.parseFloat(totalProfit) / Float.parseFloat(totalRevenue);
    }

    private Date parseDate(String date) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/d/yyyy");
        return formatter.parse(date);
    }
}
