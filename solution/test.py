import uuid
from datetime import datetime
import time
s = "01/12/2011"
t = time.mktime(datetime.strptime(s, "%d/%m/%Y").timetuple())
print(type(t))
print(t)
k = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
print(f'type={type(k)}, k={k}')


order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')

def _uuid(obj: any) -> uuid.UUID:
    return uuid.uuid5(namespace=order_ns_uuid, name=str(obj))  

vaaa = ['s1','s2',14.4]
print(_uuid(str(vaaa)))
vaaa = ['s1','s2','s4']
print(_uuid(str(vaaa)))
print(datetime.utcnow())