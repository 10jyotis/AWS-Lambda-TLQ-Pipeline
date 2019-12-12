#!/bin/bash

# JSON object to pass to Lambda Function
#json={"\"region\":\"Europe\",\"country\":\"Russia\""}
#json='{"region":"Asia","country":"Bangladesh"}'
#json='{"msg":"ServerlessComputingWithFaaS","shift":22}'
json={"\"bucketname\"":\"project-sales-data-bucket\"",""\"filename\"":\"csv-data-template/100-Sales-Records.csv\"}
echo ${json}


#echo "Invoking Lambda function using API Gateway"
#time output=`curl -s -H "ContentType: application/json" -X POST -d  $json https://so7us5v76b.execute-api.us-east-1.amazonaws.com/createCSV_dev`
#echo “”

#echo "Invoking Lambda function using AWS CLI"
#time output=`aws lambda invoke --invocation-type RequestResponse --function-name queryService --region us-east-1 --payload ${json} /dev/stdout;`
#
#echo ""
#echo "RESULT:"
#echo $output | jq
#echo ""

for i in {1..3}; do
echo "Invoking Lambda function using AWS CLI $i times"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name loadService --region us-east-1 --payload ${json} /dev/stdout;`
echo ""
echo "RESULT:"
echo $output | jq
echo ""
done
