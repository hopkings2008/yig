import base
import sys
import os
import requests
import time
import json
from urllib import unquote 
SMALL_TEST_FILE = bytes('a' * 1 * 10)  # 1M
RANGE_1 = bytes('abcdefghijklmnop' * 64 * 1024)  # 1M
RANGE_2 = bytes('qrstuvwxyz012345' * 64 * 1024)  # 1M
bucket_name = "yyytest"

# =====================================================

def pre_signed_url_listbuckets(name, client):
    url = client.generate_presigned_url(
        ClientMethod='head_bucket',
        Params={'xsid':'aaaa'},
        #Params={'Bucket':'yyytest', 'x-amz-delete-marker':'aaaa'},
        #Params={'Bucket':'yyytest'},
        HttpMethod='GET'
    )
    print url
    #response = requests.get(url)
    #print 'Presigned list buckets: ', response.text
    #assert response.status_code == 200


def pre_signed_url_putobj(name, client):
    bucket_name = "yyytest"
    key= "test.py"

    #url=client.generate_presigned_url(ClientMethod='put_object', Params={'Bucket':'yyytest', 'Key':key}, ExpiresIn=3600, HttpMethod='PUT')
    url=client.generate_presigned_url(ClientMethod='put_object', Params={'Bucket':'yyytest', 'Key':key, 'x:sid':'aaa'}, ExpiresIn=3600, HttpMethod='PUT')
    command='''curl --request PUT --upload-file {} {}'''.format(key, url)
    print(command)
    print (" Uploading with curl ...")
    #response = requests.put(url)
    #print 'Presigned create bucket: ', response.text
    #assert response.status_code == 200

def _get_object(client, object_name):
    ans = client.get_object(
        Bucket=bucket_name,
        Key=object_name,
    )
    body = ans['Body'].read()
    print 'Get object:', ans
    print 'Get body:', body

def _list_objects_v1(client):
    ans = client.list_objects(
        Bucket=bucket_name
    )
    print 'List objects:', ans

def _delete_object(client, object_name):
    client.delete_object(
        Bucket=bucket_name,
        Key=object_name,
    )
    print 'delet objects end'

def _handle_url(param):
    url = param['url']
    fileds = param['fields']
    headers = {"content-type": "application/x-www-form-urlencoded"}
    
    str = "curl -XPOST"
    for key, value in fileds.items():
        str = str + " -F " + key + "=" + value
    #    #str = str + key + "=" + value + "&"
    str = str + " -F " + 'file' + "=" + SMALL_TEST_FILE
    str = str + " " + url
    print "run : ", str
    os.system(str)
    fileds['file'] = './test.txt'
    
    response = requests.post(url,data=fileds, headers=headers)
    print 'Presigned han:', response.text


def _pre_signed_url_posttest(name, client, param):
    bucket_name = "yyytest"
    key = 'obj'+name+param
    filed_content = "123abc"
    condition_content = []
    
    print param
    fileds = { 
        "acl": "public-read-write",
    }
    fileds[param] = filed_content

    temp = '$'+param
    condition = [{"acl": "public-read-write"}]
    condition_content.append("starts-with")
    condition_content.append(temp)
    condition_content.append("")
    condition.append(condition_content)
    
    print condition

    #return client.generate_presigned_post(Bucket = bucket_name,Key = key, Fields = fileds, Conditions = [{"acl": "public-read-write"}, ["starts-with", "x:sid", ""], ["starts-with", "x:crypt", "123"]])
    return client.generate_presigned_post(Bucket = bucket_name,Key = key, Fields = fileds, Conditions = condition)


def pre_signed_url_post(name, client):
    opdict = ['x:sid', 'x:crypt', 'x:checksum', 'x:createtime', 'x:fileSize', 'x:tzoffset', 'x:segid', 'x:storagemode', 'x:index','x:begintime', 'x:endtime', 'x:eventtime', 'x:slicelength', 'x:psid', 'x:segbegintime', 'x:slicestype']
 
    for item in opdict:
        url = _pre_signed_url_posttest(name, client, item)
        bucket_name = "yyytest"
        #print "yyy", url 
        _handle_url(url)
    
    for item in opdict:
        object_name = 'obj'+name+item
        _get_object(client, object_name)

    _list_objects_v1(client)

    for item in opdict:
        object_name = 'obj'+name+item
        _delete_object(client, object_name)

def pre_signed_url_more(name, client):
    opdict = ['x:sid', 'x:crypt', 'x:checksum', 'x:createtime', 'x:fileSize', 'x:tzoffset', 'x:segid', 'x:storagemode', 'x:index','x:begintime', 'x:endtime', 'x:eventtime', 'x:slicelength', 'x:psid', 'x:segbegintime', 'x:slicestype']
    filed_content = "123abc"
    condition_content = []
    key="testmore"

    fileds = {'acl': 'public-read-write'}
    condition = [{"acl": "public-read-write"}] 
 
    num = 5
    i = 0
    for param in opdict:
        temp = '$'+param
        condition_content = []
        condition_content.append("starts-with")
        condition_content.append(temp)
        condition_content.append("")
        condition.append(condition_content)
        fileds[param] = filed_content
        i=i+1
        if i == num:
            i = 0 
            print condition
            print fileds
            res = client.generate_presigned_post(Bucket = bucket_name, Key = key, Fields = fileds, Conditions = condition)
            _handle_url(res)
            fileds = {'acl': 'public-read-write'}
            condition = [{"acl": "public-read-write"}] 
            _get_object(client, key)
            _delete_object(client, key)            
# =====================================================

PRESIGNED = [pre_signed_url_post, pre_signed_url_more]

if __name__ == '__main__':
    base.run(PRESIGNED)
