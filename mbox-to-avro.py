import mailbox
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.Parse(open("email.avro.schema").read())
writer = DataFileWriter(open("email.avro", "wb"), DatumWriter(), schema)
writer.append({"From": "Alice", "To": "Bob", "Bcc": "null", "Subject": "Testing avro"})
writer.append({"From": "Alice", "To": "Charlie", "Subject": "Testing no Bcc"})
writer.close()

reader = DataFileReader(open("email.avro", "rb"), DatumReader())
for email in reader:
    print(email)
reader.close()

mboxfile='mbox'
m = []
for message in mailbox.mbox(mboxfile):
    m.append(message)

message = m[55]

sm = str(message)
body_start = sm.find('iamunique', sm.find('iamunique')+1)
body_start = sm.find('Content-Transfer-Encoding', body_start+1)
body_start = sm.find('\n', body_start+1)+1

body_end = sm.find('From: ', body_start + 1)
body = sm[body_start:body_end]

body = body.replace("=20\n", "")
body = body.replace("=FC", "ü")
body = body.replace("=", "")

print(body)
