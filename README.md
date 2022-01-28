# Automated data extraction for Well Data with AWS Textract

Traditionally, many businesses maintained their business documents like invoice, sales memo, purchase order, vendor related document, inventory document etc in physical format. As more and more businesses are moving towards digitizing their business processes, it is becoming challenging to effectively manage all the physical documents and perform business analytics on them. For example, Oil and Gas industries have numerous documents that are generated through the exploration and production life cycle of an oil well. These documents possess key information, such as Well Number, API Well Number, Lease Number, Water Depth, etc., all these data points are used to build insights and make informed business decisions.

As, documents are usually stored in a paper format, making information retrieval time consuming and cumbersome. Also, documents that are available in a digital format, do not have adequate meta data associated to efficiently perform search and build insights.

In this post, you will learn how to build a text extraction solution leveraging Amazon Textract service, that automatically extracts text and data from scanned documents, uploaded into Amazon Simple Storage Service  (S3). And how to find insights and relationships in the extracted text using Amazon Comprehend. This data is indexed and populated into Amazon Openseacrh service to search and visualize it in Kibana dashboard




## Architecture

![Architecture Diagram](./image/Textract_architecturee.PNG)


**a. Lambda-FnA**

Lambda-FnA File
![Lambda-FnA Filess](./Lambda/Lambda-FnA.py)

**b. Lambda-FnB**

Lambda-FnB  


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
