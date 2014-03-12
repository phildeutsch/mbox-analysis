import os
import mailbox

import pandas as pd

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
    
def write_table(mboxfile, mailTable):
    for message in mailbox.mbox(mboxfile):
        mailTable.append([
            message['From'],
            message['To'],
            message['Cc'],
            message['Bcc'],
            message['Date'],
            message['Subject'],
            get_body(message)
            ])
        print(message['Subject'])
        
schema = avro.schema.Parse(open("email.avro.schema").read())
writer = DataFileWriter(open("email.avro", "wb"), DatumWriter(), schema)

path = './Archives'
mboxfiles = [os.path.join(dirpath, f)
	     for dirpath, dirnames, files in os.walk(path)
	     for f in files if f.endswith('mbox')]
mailTable = []

for mboxfile in mboxfiles:
    print(mboxfile)
    try:
        write_table(mboxfile, mailTable)
    except:
        print('Error writing mbox to pandas.')
writer.close()

m = pd.DataFrame(mailTable)
m.columns = ['From', 'To', 'Cc', 'Bcc', 'Date', 'Subject', 'Body']

