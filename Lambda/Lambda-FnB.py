import boto3
import json
from Openseacrh import Openseacrh, RequestsHttpConnection
import requests
from requests_aws4auth import AWS4Auth
import os

textract = boto3.client('textract')
sqs = boto3.client('sqs')
s3 = boto3.resource('s3')
comprehend_client = boto3.client('comprehend')

# SQS queue message from textract is received
def GetQueueUrl():
    response = sqs.get_queue_url(
        QueueName='AmazonTextract-SQS'
    )
    return response['QueueUrl']

# Pagination logic for parsing the data from pdf/image using Textract
def GetAnalysis(jobid, es, objecturl, document):
    paginationToken = None
    finished = False
    maxResults = 1000
    responselist = []
    while finished == False:

        response = None

        if paginationToken == None:

            response = textract.get_document_analysis(
                JobId=jobid, MaxResults=maxResults
            )
        else:
            response = textract.get_document_analysis(
                JobId=jobid, MaxResults=maxResults, NextToken=paginationToken
            )

        responselist.append(response)
        # Get next page token or stop processing
        if 'NextToken' in response:
            paginationToken = response['NextToken']
            # print(paginationToken)
        else:
            # print('end of document')
            finished = True
    pagecount = response['DocumentMetadata']['Pages']
    pagedata = dict()
	# read the raw textract response and extract required attributes
    for pageno in range(1, pagecount + 1):
        key_value_ids = {}
        key_value_set_block = []
        word_block = []
        line_block = []


        for response in responselist:
            blocks = response['Blocks']
            for each_dict in blocks:
                if each_dict['BlockType'] == 'KEY_VALUE_SET' and each_dict['Page'] == pageno:
                    key_value_set_block.append(each_dict)
                elif each_dict['BlockType'] == 'WORD' and each_dict['Page'] == pageno:
                    word_block.append(each_dict)
                elif each_dict['BlockType'] == 'LINE' and each_dict['Page'] == pageno:
                    line_block.append(each_dict)
      
        each_dict = {}
        each_relation = []

        # getting IDs of key values
        for each_dict in key_value_set_block:
            if 'KEY' in each_dict['EntityTypes']:
                key_id = each_dict['Id']
                for each_relation in each_dict['Relationships']:
                    if each_relation['Type'] == 'VALUE':
                        value_id = each_relation['Ids'][0]
                key_value_ids[key_id] = value_id

        key_value_child_ids = []
        each_dict = {}
        each_relation = []

        # Getting child IDs of key values
        for key, value in key_value_ids.items():
            for each_dict in key_value_set_block:
                if 'KEY' in each_dict['EntityTypes']:
                    if each_dict['Id'] == key:
                        for each_key_relation in each_dict['Relationships']:
                            if each_key_relation['Type'] == 'CHILD':
                                key_child_id = each_key_relation['Ids']

                if 'VALUE' in each_dict['EntityTypes']:
                    if each_dict['Id'] == value:
                        if 'Relationships' in each_dict:
                            for each_child_relation in each_dict['Relationships']:
                                if each_child_relation['Type'] == 'CHILD':
                                    value_child_id = each_child_relation['Ids']
                        else:
                            value_child_id = []
            sublist = []
            sublist.append(key_child_id)
            sublist.append(value_child_id)
            key_value_child_ids.append(sublist)

        final_dict = {}

        # Getting words of key value

        for each_list in key_value_child_ids:
            key_list = each_list[0]
            key_string = ''
            for each_key_sublist in key_list:
                for each_word_dict in word_block:
                    if each_word_dict['Id'] == each_key_sublist:
                        key_string += each_word_dict['Text'] + ' '

            value_list = each_list[1]
            value_string = ''
            for each_value_sublist in value_list:
                for each_word_dict in word_block:
                    if each_word_dict['Id'] == each_value_sublist:
                        value_string += each_word_dict['Text'] + ' '

            final_dict[key_string] = value_string
        print(final_dict)

        key_value_text = ""
        for key, value in final_dict.items():
            key_value_text += key + " " + value + " "


        report_type_dict = {'SUNDRY NOTICES AND REPORTS ON WELL': 'WELL REPORT',
                            'PROPOSED WELLBORE SKETCH': 'WELL SCHEMATIC REPORT',
                            'WELL SUMMARY REPORT': 'WELL SUMMARY REPORT',
                            'COMPLETION OR RECOMPLETION REPORT': 'WELL COMPLETION REPORT'}

#        final_report_dict = {}
        report_type = 'unknown report'
        # Getting type of report
        for key, value in report_type_dict.items():
            for each_line_dict in line_block:
                if key in each_line_dict['Text']:
                    report_type = value
                    break

        # depending on type of report, fetching appropriate values

        report_othrs_dict = 
{'API WELL NUMBER': ['API WELL NUMBER', 'AP| WELL NUMBER', 'API NO', 'AP| NO'],
 'LEASE NAME': 'LEASE NAME', 'COUNTY': 'COUNTY', 'WELL NUMBER': 'WELL NO',
 'FIELD NAME': 'FIELD NAME', 'FIELD NUMBER': ['FIELD NUMBER', 'FIELD NO'],
 'AREA NAME': 'AREA NAME', 'BLOCK NUMBER': 'BLOCK NUMBER',
 'OPERATOR NAME': 'OPERATOR NAME','NAME OF OPERATOR': ['NAME OF OPERATOR', 'NAME OF CPERATOR'], 'LEASE NO': ['LEASE NO'], 'ADJACENT STATE': ['ADJACENT STATE'], 'LOCATION OF WELL': ['LOCATION OF WELL'],'ADDRESS OF OPERATOR': ['ADDRESS OF OPERATOR'], 'UNIT AGREEMENT': ['UNIT AGREEMENT'], 'WATER DEPTH': ['WATER DEPTH'],
'AREA & BLOCK': ['AREA & BLOCK', 'AREA& BLOCK'],  'FIELD': ['FIELD']
                             }



        formatted_dict = {}
        formatted_dict['REPORT TYPE'] = report_type
        formatted_dict['OBJECT URL'] = objecturl

#Calling comprehend to get entity types
        comprehend_response = comprehend_client.detect_entities(
                            Text = key_value_text,
                            LanguageCode='en',
                            EndpointArn ='arn:aws:comprehend:us-east-1:694650458022:entity-recognizer-endpoint/comprehendep')
        
        comprehend_dict = {}
        print(comprehend_response['Entities'])
        for response_list in comprehend_response['Entities']:
            comprehend_dict[response_list['Text']] = response_list['Type']
        
#Correcting values using comprehend response
        for final_dict_key,value in final_dict.items():
            for comprehend_dict_key in comprehend_dict.keys():
                if comprehend_dict_key in final_dict_key:
                    print(comprehend_dict_key)
                    print(value)
                    print('true')
                    formatted_dict[comprehend_dict[comprehend_dict_key]] = value

        # print ('Pages: {}'.format(response['DocumentMetadata']['Pages']))

        print(formatted_dict)
        if '' in formatted_dict:
            del formatted_dict['']
        pagedata[pageno] = formatted_dict

    for key, value in pagedata.items():
        datajson = json.loads(json.dumps(value))
        print(datajson)

        Post2ES(es, datajson)


def DeleteMsg(SQSQueueUrl, ReceiptHandle):
    sqs.delete_message(
        QueueUrl=SQSQueueUrl,
        ReceiptHandle=ReceiptHandle
    )

# calling Openseacrh to populate the data(Post2ES)
def Post2ES(es, esData):
    print("posting into ES")
    print(es)
    resp_index = es.index(index="well-reports", doc_type="_doc", body=esData)
    print(resp_index)


def lambda_handler(event, context):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    es= ''
    host = os.environ['esDomain']
    es = Openseacrh(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    SQSQueueUrl = GetQueueUrl()
    for record in event['Records']:
        bucket = json.loads(record['body'])['DocumentLocation']['S3Bucket']
        document = json.loads(record['body'])['DocumentLocation']['S3ObjectName']
        objecturl = 'https://' + bucket + '.s3.amazonaws.com/' + document
        JobId = json.loads(record['body'])['JobId']
        JobStatus = json.loads(record['body'])['Status']
        ReceiptHandle = record['receiptHandle']
        if JobStatus == 'SUCCEEDED':
            GetAnalysis(JobId, es, objecturl, document)
            DeleteMsg(SQSQueueUrl, ReceiptHandle)
