import os
import json
import time

from awscrt import io, mqtt
from awsiot import mqtt_connection_builder as builder

evt_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(evt_loop_group)
client_bootstrap = io.ClientBootstrap(evt_loop_group, host_resolver)

connection = builder.mtls_from_path(endpoint=os.getenv('IOT_ENDPOINT'),
                                    cert_filepath="cert.pem.crt",
                                    pri_key_filepath="private.pem.key",
                                    client_bootstrap=client_bootstrap,
                                    ca_filepath="AmazonRootCA1.pem",
                                    client_id="simplething")

connection.connect().result()  # Wait for promise

for msg_num in range(1,10):
    message = {"message": "Hello world [{}]".format(msg_num)}
    connection.publish(topic="test/testing",
                       payload=json.dumps(message),
                       qos=mqtt.QoS.AT_LEAST_ONCE)
    print("Sent: '" + json.dumps(message))
    time.sleep(1)

connection.disconnect().result()