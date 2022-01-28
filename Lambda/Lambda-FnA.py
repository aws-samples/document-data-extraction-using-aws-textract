import boto3
import urllib.parse
import os
textract = boto3.client('textract')
sqs = boto3.client('sqs')
sns = boto3.client('sns')

# create sns topic/sqs queue
def createSqsSns():
    sqsname = "AmazonTextract-SQS"
    snsname = "AmazonTextract-SNS"
    sqsresponse = sqs.create_queue(QueueName=sqsname)
    snsresponse = sns.create_topic(Name=snsname)  
    return sqsresponse,snsresponse
# subscribe the sns topic to sqs
def subscribeSqsToSns(sqsresponse,snsresponse):
    sqsattr = sqs.get_queue_attributes(
            QueueUrl=sqsresponse['QueueUrl'],
            AttributeNames=['QueueArn']
    )  
    sns.subscribe(
    TopicArn=snsresponse['TopicArn'],
    Protocol='sqs',
    Endpoint=sqsattr['Attributes']['QueueArn'],
    Attributes={
        'RawMessageDelivery': 'true'
    },
    ReturnSubscriptionArn=True
    )

# allowing sns messages to sqs
    policy = """{{
    "Version":"2012-10-17",
    "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
    ]
    }}""".format(sqsattr['Attributes']['QueueArn'], snsresponse['TopicArn'])
 
    sqs.set_queue_attributes(
            QueueUrl = sqsresponse['QueueUrl'],
            Attributes = {
                'Policy' : policy
    })

            
def lambda_handler(event,context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    document = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    roleArn = os.environ['textractRole']
   
    sqsresponse,snsresponse=createSqsSns()
    subscribeSqsToSns(sqsresponse,snsresponse)
    
	# calling textract to start document analysis
    startanalysis = textract.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': bucket,
            'Name': document
        }
    },
    FeatureTypes=[
        'TABLES','FORMS',
    ],
    JobTag='jobid',
    NotificationChannel={
        'SNSTopicArn': snsresponse['TopicArn'],
        'RoleArn': roleArn
    }  
    )
    
    response = textract.get_document_analysis(
        JobId=startanalysis['JobId']
    )
	# acknowledgement for successful acceptance of the job by Textract service
    print(response)
