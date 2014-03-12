import os
import mailbox
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def get_body(message):
    try:
        sm = str(message)
        body_start = sm.find('iamunique', sm.find('iamunique')+1)
        body_start = sm.find('Content-Transfer-Encoding', body_start+1)
        body_start = sm.find('\n', body_start+1)+1
    
        body_end = sm.find('From: ', body_start + 1)
        body = sm[body_start:body_end]

        body = body.replace("=20\n", "")
        body = body.replace("=FC", "ü")
        body = body.replace("=", "")
    except:
        body = "N/A"
    
    return body

def write_mbox(mboxfile, writer):
    for message in mailbox.mbox(mboxfile):
        writer.append({
            'From': message['From'],
            'To': message['To'],
            'Cc': message['Cc'],
            'Bcc': message['Bcc'],
            'Date': message['Date'],
            'Subject': message['Subject'],
            'Body': get_body(message)
            })
        print(message['Subject'])
   
schema = avro.schema.Parse(open("email.avro.schema").read())
writer = DataFileWriter(open("email.avro", "wb"), DatumWriter(), schema)

path = './Archives'
mboxfiles = [os.path.join(dirpath, f)
	     for dirpath, dirnames, files in os.walk(path)
	     for f in files if f.endswith('mbox')]
for mboxfile in mboxfiles:
    write_mbox(mboxfile, writer)
    print(mboxfile)
writer.close()

#reader = DataFileReader(open("email.avro", "rb"), DatumReader())
#for email in reader:
#    print(email['Date'])
#reader.close()
