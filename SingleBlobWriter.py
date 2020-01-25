import json
from azure.storage.blob import PageBlobService,BlockBlobService,BlobBlock,BlobBlockList
import io
import copy
import requests

from multiprocessing import Process
from threading import Thread
class SingleBlobWriter:
    def __init__(self,accountname, accountkey, container, blob_name,key):
        self.acc_name = accountname
        self.acc_key = accountkey
        self.container = container
        self.key=key
        self.bbs = BlockBlobService(account_name = self.acc_name,account_key = self.acc_key)
        self.key = key
        self.blob_name = blob_name
        self.bbs.create_blob_from_text(self.container,self.blob_name,"")
    
    def write(self,msg):
        data = json.loads(msg)
        #assert(self.key in data.keys())
        pkey = data[self.key]        

        self.bbs.put_block(self.container,self.blob_name,io.StringIO(json.dumps(data)+"\r\n"),pkey)

    def commit(self):
        block_list = self.bbs.get_block_list(self.container,self.blob_name,block_list_type="all")
        ids =  list(set([x.id for x in block_list.uncommitted_blocks]+[x.id for x in block_list.committed_blocks])) 
        new_list = [BlobBlock(ii) for ii in ids]
        self.bbs.put_block_list(self.container,self.blob_name,new_list)




    
