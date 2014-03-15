import os
import mailbox
import re

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def clean_addresses(addresses, lookupcsv):
    addressList = addresses.split(',')
    cleanList = []
    for address in addressList:
        cleanList.append(clean_address(address, lookupcsv))
    return cleanList

def clean_address(address, lookupcsv):
#    print('Dirty:\t' + address)
    address = address.replace("<", "")
    address = address.replace(">", "")
    address = address.replace("\"", "")
    address = address.replace("\n", " ")
    address = address.replace("MAILER-DAEMON", "")
    address = address.lower().strip()

    with open(lookupcsv, 'rt') as lookupfile:
        lookupdata = lookupfile.readlines()
    for line in lookupdata:
        name = line.split(',')[0]
        if address == name:
            address = line.split(',')[-1].strip()

    email = None
    for word in address.split(' '):
        emailRegex = re.compile(
            "^[a-zA-Z0-9._%-]+@[a-zA-Z0-9._%-]+.[a-zA-Z]{2,6}$"
            )
        email = re.match(emailRegex, word)
        if email is not None:
            cleanEmail = email.group(0)
    if email is None:
        if address.split(' ')[-1].find('@') > -1:
            cleanEmail = address.split(' ')[-1].strip()
        elif address.split(' ')[-1].find('?') > -1:
            cleanEmail = 'n/a'
        else:
            cleanEmail = address
              
#    print('Clean:\t' + cleanEmail)
    return cleanEmail

def get_body(message):
    try:
        sm = str(message)
        body_start = sm.find('iamunique', sm.find('iamunique')+1)
        body_start = sm.find('Content-Transfer-Encoding', body_start+1)
        body_start = sm.find('\n', body_start+1)+1
    
        body_end = sm.find('From: ', body_start + 1)
        if body_end == -1:
            body_end = sm.find('iamunique', body_start + 1)
            body_end = sm.find('\n', body_end - 25)
        body = sm[body_start:body_end]

        body = body.replace("=20\n", "")
        body = body.replace("=FC", "ü")
        body = body.replace("=F6", "ö")
        body = body.replace("=84", "\"")
        body = body.replace("=94", "\"")
        body = body.replace("=96", "-")
        body = body.replace("=92", "\'")
        body = body.replace("=93", "\"")
        body = body.replace("=E4", "ä")
        body = body.replace("=DF", "ss")
        body = body.replace("=", "")
    except:
        body = "N/A"
    
    return body

def write_avro(mboxfile, writer):
    for message in mailbox.mbox(mboxfile):
        cleanFrom = clean_address(message['From'], 'name_to_address.csv')
        writer.append({
            'From': cleanFrom,
            'To': message['To'],
            'Cc': message['Cc'],
            'Bcc': message['Bcc'],
            'Date': message['Date'],
            'Subject': message['Subject'],
            'Body': get_body(message)
            })
        print(cleanFrom)

schema = avro.schema.Parse(open("email.avro.schema").read())
writer = DataFileWriter(open("email.avro", "wb"), DatumWriter(), schema)
   
path = '../emails/Archives'
mboxfiles = [os.path.join(dirpath, f)
	     for dirpath, dirnames, files in os.walk(path)
	     for f in files if f.endswith('mbox')]
mailTable = []
#print(mboxfiles)

for mboxfile in mboxfiles:
#    print(mboxfile)
    try:
       write_avro(mboxfile, writer)
    except:
        print('Error writing mbox to avro.')        

writer.close()

#reader = DataFileReader(open("email.avro", "rb"), DatumReader())
#for email in reader:
#   print(email['Subject'])
#   reader.close()

