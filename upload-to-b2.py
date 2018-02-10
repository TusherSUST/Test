
# encoding=utf8
import os
import sys
import json
import base64
import urllib2
import hashlib
import logging
import shutil

import requests

from google.cloud import storage
from sys import argv

logging.basicConfig(filename="logger.log", level=logging.INFO)

id_and_key = 'fd64b886f6d6:00071a65c9d7c0b9b804c26966c447775f7069fea5'
basic_auth_string = 'Basic ' + base64.b64encode(id_and_key)
auth_header = { 'Authorization': basic_auth_string }

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
script, start, end, b2_bucketId = argv
start = int(start)
end = int(end)

cwd = os.getcwd()

client = storage.Client()
bucket = client.get_bucket("audiobook_knigi")

def getBucketPrefix(value):
    return "AudioBooks/" + str(value)

def downloadBlob(source_blob_name, destination_file_name):
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

def uploadToB2(fileData, uploadPath, contentType, bookId):
    success = False
    fileId = -1
    for i in range(0,6):
        if success == True:
            break
        try:
            authReq = urllib2.Request(
                'https://api.backblazeb2.com/b2api/v1/b2_authorize_account',
                headers = auth_header
                )
            authResp = urllib2.urlopen(authReq)
            authRespData = json.loads(authResp.read())
            authResp.close()

            b2_authToken = authRespData['authorizationToken']
            b2_apiUrl = authRespData['apiUrl']

            uploadUrlReq = urllib2.Request(
            	'%s/b2api/v1/b2_get_upload_url' % b2_apiUrl,
            	json.dumps({ 'bucketId' : b2_bucketId }),
            	headers = { 'Authorization': b2_authToken }
            	)
            uploadUrlresp = urllib2.urlopen(uploadUrlReq)
            uploadUrlrespData = json.loads(uploadUrlresp.read())
            uploadUrl = uploadUrlrespData['uploadUrl']
            uploadAuthToken = uploadUrlrespData['authorizationToken']
            uploadUrlresp.close()



            headers = {
                'Authorization' : uploadAuthToken,
                'X-Bz-File-Name' :  uploadPath,
                'Content-Type' : contentType,
                'X-Bz-Content-Sha1' : 'do_not_verify'
                }
            uploadReq = requests.post(uploadUrl, data=fileData, headers=headers)
            uploadResp = json.loads(uploadReq.text)
            fileId = uploadResp['fileId']
            uploadReq.close()
            success = True
        except:
            e = sys.exc_info()[0]
            logging.info(e)

    if fileId == -1:
        logging.info("Failed Uploading Book: " + str(bookId))
    return fileId

def getB2Url(fileId):
    return " https://f000.backblazeb2.com/b2api/v1/b2_download_file_by_id?fileId=" + fileId

for value in range(start, end+1):
    dir = os.path.join(cwd, str(value))

    try:
        os.makedirs(dir)
    except OSError as e:
        if e.errno != 17:
            raise
    deskDir = os.path.join(dir, "tmpDesc.json")
    downloadBlob(getBucketPrefix(value) + "/bookDescription.json", deskDir)

    with open(deskDir) as infile:
        descData = json.load(infile)
    bookDesc = {
        "bookName": descData['bookName'],
        "bookId": descData['bookId'],
        "playlist": []
    }

    imageDir = os.path.join(dir, "bookImage.png")
    downloadBlob(getBucketPrefix(value) + "/bookImage.png", imageDir)

    image = open(imageDir, "rb")
    imageFileId = uploadToB2(image, "AudioBooks/" + str(value) + "/bookImage.png", 'image/png', value)
    image.close()
    bookDesc['bookImageUrl'] = getB2Url(imageFileId)

    for idx, chapter in enumerate(descData['chapterList']):
        chapterDir = os.path.join(dir, chapter['comment'] + ".mp3")
        downloadBlob(getBucketPrefix(value) + "/" + chapter['comment'] + ".mp3", chapterDir)

        mp3File = open(chapterDir, 'rb')
        mp3FileId = uploadToB2(mp3File, "AudioBooks/" + str(value) + "/" + str(idx) + ".mp3", 'audio/mpeg', value)
        mp3File.close()
        bookDesc['playlist'].append({"comment": chapter['comment'], "file": getB2Url(mp3FileId)})

    with open(os.path.join(dir, "bookDescription.json"), 'wb') as outfile:
        json.dump(bookDesc, outfile)

    curDesc = open(os.path.join(dir, "bookDescription.json"), 'r')
    curDescData = curDesc.read()
    curDesc.close()

    descFileId = uploadToB2(curDescData, "AudioBooks/" + str(value) + "/bookDescription.json", 'application/json', value)
    logging.info("Completed Book : " + str(value))
    shutil.rmtree(dir, ignore_errors=False, onerror=None)
