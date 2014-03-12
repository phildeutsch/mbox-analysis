import mailbox
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

def get_body(message):
    sm = str(message)
    body_start = sm.find('iamunique', sm.find('iamunique')+1)
    body_start = sm.find('Content-Transfer-Encoding', body_start+1)
    body_start = sm.find('\n', body_start+1)+1

    body_end = sm.find('From: ', body_start + 1)
    body = sm[body_start:body_end]

    body = body.replace("=20\n", "")
    body = body.replace("=FC", "ü")
    body = body.replace("=", "")
    
    return body

schema = avro.schema.Parse(open("email.avro.schema").read())
writer = DataFileWriter(open("email.avro", "wb"), DatumWriter(), schema)


mboxfile='mbox'
m = []
for message in mailbox.mbox(mboxfile):
    m.append(message)
    msg_To = message['To']
    msg_From = message['From']
    msg_Bcc = message['Bcc']
    msg_Subj= message['Subject']
    msg_Body= get_body(message)

    writer.append({
        'From': msg_From,
        'To': msg_To,
        'Bcc': msg_Bcc,
        'Subject': msg_Subj,
        'Body': msg_Body
        })
    
writer.close()

reader = DataFileReader(open("email.avro", "rb"), DatumReader())
for email in reader:
    print(email['Subject'])
reader.close()
