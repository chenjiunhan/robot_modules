from kafka import KafkaConsumer
from json import loads
import io
import numpy as np    
from PIL import Image
import matplotlib.pyplot as plt


def value_deserializer(image_binary):
    print(type(image_binary))
    arr = np.frombuffer(image_binary, dtype=np.uint8)
    #img = Image.open(io.BytesIO(image_binary))
    #arr = np.asarray(img)

    return arr
    

consumer = KafkaConsumer(
    'image',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=value_deserializer)

for message in consumer:
    message = message.value

    img = message.reshape((180, 320, 3))

    plt.imshow(img)
    plt.show()
