import cv2

from time import sleep
from json import dumps
from kafka import KafkaProducer

def value_serializer(image):
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    return image.tobytes()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=value_serializer)

cam = cv2.VideoCapture(0)

cam.set(cv2.CAP_PROP_FRAME_WIDTH, 320)
cam.set(cv2.CAP_PROP_FRAME_HEIGHT, 180)

cv2.namedWindow("test")

img_counter = 0

while True:
    ret, frame = cam.read()
    cv2.imshow("test", frame)
    if not ret:
        break
    k = cv2.waitKey(1)

    if k%256 == 27:
        # ESC pressed
        print("Escape hit, closing...")
        break
    elif k%256 == 32:
        # SPACE pressed
        print("image to kafka")
        print(frame.dtype)
        producer.send('image', value=frame)
        print(frame.shape)

cam.release()

cv2.destroyAllWindows()