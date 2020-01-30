# Introduction
The code in this library allows to use a container in azure blob storage as a Document Store, without splitting every document into a separate blob. This allows for good performance when bulk-loading data into PowerBI, a spark dataframe, a sql table etc without having to worry about duplicate records/rows

 
# Usage


Create BlobWriter object.
```python
from blobwriter import BlobWriter
bw = BlobWriter(acc_name,acc_key,container,partition_name,key,max_blocks=50000, out_fn=lambda x: json.dumps(x)+ "\n",suffix=".json")
```
- acc_name : azure blob storage account name
- acc_key : blob storage access key
- container : blob storage container
- partition name : Name of the partition. Usually one file per partition. If the number of documents in a partition exceeds max_blocks, a new file will be created automatically and the partition is split into multiple files.
- key : "Primary key" for documents. Writer will look for this attribute in message to use as a key for the documents.
- suffix : File extension.
- max_blocks : (See "partition name")
- out_fn : Function that processes message into a string before storing it.

Write message:
```python
bw.write_batch(message)
```

- message : json-formatted string. Should be a "list of structs"

# Dependencies
- azure-storage-blob==1.5.0
