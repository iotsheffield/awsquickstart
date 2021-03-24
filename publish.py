from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time
import json

evt_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(evt_loop_group)
client_bootstrap = io.ClientBootstrap(evt_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(endpoint="ENDPOINT",
                                                         cert_filepath="cert.pem.crt",
                                                         pri_key_filepath="private.pem.key",
                                                         client_bootstrap=client_bootstrap,
                                                         ca_filepath="AmazonRootCA1.pem",
                                                         client_id="simplething",
                                                         clean_session=False,
                                                         keep_alive_secs=6)

mqtt_connection.connect().result()  # Wait for promise

for i in range(1,10):
    message = {"message" : "Hello world [{}]".format(i)}
    mqtt_connection.publish(topic="test/testing", payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
    print("Published: '" + json.dumps(message))
    time.sleep(1)

mqtt_connection.disconnect().result()