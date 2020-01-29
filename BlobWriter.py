import json
from azure.storage.blob import PageBlobService,BlockBlobService,BlobBlock,BlobBlockList
import io
import copy
import requests

from multiprocessing import Process
from threading import Thread


class BlobWriter:
    def __init__(self,accountname, accountkey, container, blob_name,key, max_blocks=50000, out_fn=lambda x: json.dumps(x)+ "\n",suffix=".json"):
        self.acc_name = accountname
        self.acc_key = accountkey
        self.container = container
        self.key=key
        self.bbs = BlockBlobService(account_name = self.acc_name,account_key = self.acc_key)
        self.key = key
        self.blob_name = blob_name
        self.max_blocks = max_blocks
        self.out_fn = out_fn
        self.suffix = suffix

        self.all_blobs = [x.name for x in self.bbs.list_blobs(self.container,prefix=self.blob_name) ] 

        if self.blob_name + "_part_1"+self.suffix not in self.all_blobs:
            assert(not self.all_blobs)
            self.bbs.create_blob_from_text(self.container,self.blob_name+"_part_1"+self.suffix,"")
            self.all_blobs = [self.blob_name + "_part_1"+self.suffix]
        else:
            newblobs=[self.blob_name + "_part_1"+self.suffix ]
            k=2
            while True:
                if self.blob_name + "_part_{0:}"+self.suffix.format(k) not in self.all_blobs:
                    break
                else:
                    newblobs.append(self.blob_name + "_part_{0:}".format(k)+self.suffix )
                k+=1
            self.all_blobs=newblobs
        print(self.all_blobs)


    #messages should be in ascending order by timestamp
    def write_batch(self,msgs):
        data_tmp = json.loads(msgs)
        #assert(len(data_tmp) < self.max_blocks)
        data = { data_tmp[i][self.key]:data_tmp[i] for i in range(0,len(data_tmp)) }


        #Try to update db...
        for n in range(0,len(self.all_blobs)):

            part = "_part_{0:}".format(n+1)+self.suffix
            block_list = self.bbs.get_block_list(self.container,self.blob_name+part,block_list_type="all")
            ids =  list(set([x.id for x in block_list.committed_blocks] + [x.id for x in block_list.committed_blocks]))

            common_keys = list(set(data.keys()) & set(ids))
            for j in range(0,len(common_keys)):
                self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[common_keys[j]])),common_keys[j])
            new_list = [BlobBlock(ii) for ii in ids]
            self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

            newkeys = list(set(data.keys())-set(ids))
            data = {newkeys[i]:data[newkeys[i]] for i in range(0,len(newkeys))}
            if not data:
                break

        #Try to write new record to one of the existing files...
        if data:
            for n in range(0,len(self.all_blobs)):
                part = "_part_{0:}".format(n+1)+self.suffix

                block_list = self.bbs.get_block_list(self.container,self.blob_name+part,block_list_type="all")
                ids =  list(set([x.id for x in block_list.committed_blocks] + [x.id for x in block_list.committed_blocks]))


                space_left = self.max_blocks - len(ids)
                



                keys = list(data.keys())
                max_ins = min(len(keys),space_left)
                
                keys = [keys[i] for i in range(0,max_ins)]

                new_list = [BlobBlock(ii) for ii in ids]
                for j in range(0,max_ins):
                    key=keys[j]
                    self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key)
                    new_list.append(BlobBlock(key))
                self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

                newkeys = list(set(data.keys())-set(keys))
                data = {newkeys[i]:data[newkeys[i]] for i in range(0,len(newkeys))}
                if not data:
                    break
        

        num_new =1
        while data:
            #Create new file and write record
            nn = len(self.all_blobs)+num_new
            part = "_part_{0:}".format(nn)+self.suffix
            self.all_blobs.append(self.blob_name+part)
            self.bbs.create_blob_from_text(self.container,self.blob_name+part,"")
            keys = list(data.keys())
            new_list  = []

            used_keys = []
            for key in keys:
                self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key)
                new_list.append(BlobBlock(key))
                used_keys.append(key)
            self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

            keys_left = list(set(keys)-set(used_keys))
            data = {key:data[key] for key in keys_left}

            num_new +=1
        
     #messages should be in ascending order by timestamp
    def write_batch_threaded(self,msgs):
        data_tmp = json.loads(msgs)
        #assert(len(data_tmp) < self.max_blocks)
        data = { data_tmp[i][self.key]:data_tmp[i] for i in range(0,len(data_tmp)) }


        #Try to update db...
        for n in range(0,len(self.all_blobs)):

            part = "_part_{0:}".format(n+1)+self.suffix

            block_list = self.bbs.get_block_list(self.container,self.blob_name+part,block_list_type="all")
            ids =  list(set([x.id for x in block_list.committed_blocks] + [x.id for x in block_list.committed_blocks]))

            common_keys = list(set(data.keys()) & set(ids))

            threads = []
            for j in range(0,len(common_keys)):

                t = Thread(target=self.bbs.put_block,args=(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[common_keys[j]])),common_keys[j]))
                t.start()
                threads.append(t)
                #self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[common_keys[j]])),common_keys[j])

            while threads:
                time.sleep(0.01)
                threads = [t for t in threads if t.isAlive()]
            
            new_list = [BlobBlock(ii) for ii in ids]
            self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

            newkeys = list(set(data.keys())-set(ids))
            data = {newkeys[i]:data[newkeys[i]] for i in range(0,len(newkeys))}
            if not data:
                break

        #Try to write new record to one of the existing files...
        if data:
            for n in range(0,len(self.all_blobs)):
                part = "_part_{0:}".format(n+1)+self.suffix

                block_list = self.bbs.get_block_list(self.container,self.blob_name+part,block_list_type="all")
                ids =  list(set([x.id for x in block_list.committed_blocks] + [x.id for x in block_list.committed_blocks]))


                space_left = self.max_blocks - len(ids)
                



                keys = list(data.keys())
                max_ins = min(len(keys),space_left)
                
                keys = [keys[i] for i in range(0,max_ins)]

                new_list = [BlobBlock(ii) for ii in ids]
                threads = []
                for j in range(0,max_ins):
                    key=keys[j]
                    #self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key)
                    new_list.append(BlobBlock(key))

                    t = Thread(target=self.bbs.put_block,args=(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key))
                    t.start()
                    threads.append(t)
                #self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[common_keys[j]])),common_keys[j])

                while threads:
                    time.sleep(0.01)
                    threads = [t for t in threads if t.isAlive()]

                self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

                newkeys = list(set(data.keys())-set(keys))
                data = {newkeys[i]:data[newkeys[i]] for i in range(0,len(newkeys))}
                if not data:
                    break
        

        num_new =1
        while data:
            #Create new file and write record
            nn = len(self.all_blobs)+num_new
            part = "_part_{0:}".format(nn)+self.suffix
            self.all_blobs.append(self.blob_name+part)
            self.bbs.create_blob_from_text(self.container,self.blob_name+part,"")
            keys = list(data.keys())
            new_list  = []

            used_keys = []
            threads = []
            for key in keys:
                #self.bbs.put_block(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key)
                new_list.append(BlobBlock(key))
                used_keys.append(key)
                t = Thread(target=self.bbs.put_block,args=(self.container,self.blob_name+part,io.StringIO(self.out_fn(data[key])),key))
                t.start()
                threads.append(t)

            while threads:
                time.sleep(0.01)
                threads = [t for t in threads if t.isAlive()]
                
            self.bbs.put_block_list(self.container,self.blob_name+part,new_list)

            keys_left = list(set(keys)-set(used_keys))
            data = {key:data[key] for key in keys_left}

            num_new +=1
        




        




if __name__=="__main__":

    acc_name = "alekstest"
    acc_key = ""
    container = "dbtest"
    partition_name = "test"
    key = "pkey"

    bw = BlobWriter(acc_name,acc_key,container,partition_name,key,suffix=".csv",max_blocks=50000)
    


    import time
    n=10
    m=20000
    import numpy as np
    data = {}
    t0=time.time()

    num_msgs = 100
    num_sent=0
    for i in range(0,n):

        datamsg = []
        for j in range(0,num_msgs):
            key = "{0:06d}".format(np.random.randint(m))
            msg = np.random.randint(1000)
            datamsg.append({"pkey":key,"data":msg})
            data[key]=msg

        

        bw.write_batch_threaded(json.dumps(datamsg))

        #time.sleep(0.01)
        num_sent += num_msgs
        print(i,n, num_sent/(time.time()-t0))

    time.sleep(5)
    print(data)

    
