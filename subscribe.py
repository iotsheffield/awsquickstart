from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import os
import threading


def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))


event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

mqtt_connection = mqtt_connection_builder.mtls_from_path(endpoint=os.getenv('IOT_ENDPOINT'),
                                                         cert_filepath="cert.pem.crt",
                                                         pri_key_filepath="private.pem.key",
                                                         client_bootstrap=client_bootstrap,
                                                         ca_filepath="AmazonRootCA1.pem",
                                                         client_id="simplething")

mqtt_connection.connect().result()

subscribe_future, packet_id = mqtt_connection.subscribe(
    topic="test/testing",
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)

subscribe_result = subscribe_future.result()

threading.Event().wait();

# while True:
#     pass

mqtt_connection.disconnect().result()