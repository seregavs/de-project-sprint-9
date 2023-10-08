import uuid
from datetime import datetime

order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')

def _uuid(obj: any) -> uuid.UUID:
    return uuid.uuid5(namespace=order_ns_uuid, name=str(obj))  

vaaa = ['s1','s2','s3']
print(_uuid(str(vaaa)))
vaaa = ['s1','s2','s4']
print(_uuid(str(vaaa)))
print(datetime.utcnow())