from dojot.module import Messenger, Config, Auth
from dojot.module.logger import Log
import json
import requests
import time

LOGGER = Log().color_log()

class IoTAgent:

    def __init__(self):
        self.config = Config()
        self.messenger = Messenger("IoTAgent-Python", self.config)
        self.auth = Auth(self.config)
    
    def init(self):
        """
            Initialize kafka listeners and producers
        """

        LOGGER.debug(f"Initialiazing IoTAgent messenger...")
        self.messenger.init()
        LOGGER.debug(f"... IoTAgent messenger was successfully initialized")
        LOGGER.debug(f"Creating channel for device-data subject...")
        self.messenger.create_channel(self.config.dojot['subjects']['device_data'], "w")
        LOGGER.debug(f"... channel for device-data subject was created")

        LOGGER.debug(f"Registering callback for DeviceManager device subject...")
        self.messenger.on(self.config.dojot['subjects']['devices'], "message", self.callback)
        LOGGER.debug(f"...callback registered")

    def callback(self, tenant, message):
        """
            Callback for DeviceManager subjects

            :type tenant: str
            :param tenant: The tenant associated to the message
            :type msg: dict
            :param msg: Message received from DeviceManager
        """

        try:
            parsed = json.loads(message)
        except:
            LOGGER.error(f"Device event is not a valid JSON. Ignoring it")

        event_type = "device.{}".format(parsed['event'])
        self.messenger.emit("iotagent.device", tenant, event_type, parsed)
    
    def get_device(self, tenant, deviceid):
        """
            Given a device id and its associated tenant, retrieve its full configuration.

            :type tenant: str
            :param tenant: The tenant associated to the device
            :type deviceid: str
            :param devideid: id related to the device

            :rtype: dict
            :return: Full configuration of device
        """

        url = "http://172.20.0.9:5000/device/{}".format(deviceid)
        retry_counter = 2
        ret = None
        
        while retry_counter > 0:
            try:
                ret = requests.get(url, headers={'authorization': "Bearer + " + self.auth.get_access_token(tenant)})
                retry_counter = 0;
                return ret.json()
            except:
                if ret.status_code is 404:
                    LOGGER.error("Unknown device")
                retry_counter -= 1

    def check_complete_metafields(self, tenant, deviceid, metadata):
        """
            Internal method used to fill up required fields when informing updates to dojot

            :type tenant: str
            :param tenant: Tenant associated to the meta
            :type deviceid: str
            :param devideid: Id of the device
            :type metadata: dict
            :param metadata: Metadata dict to check fields

            :rtype: None or dict
            :return: Metadata fields completed with all fields needed
        """

        if not metadata:
            LOGGER.error(f"Failed to vadidate event. Device metadata must be passed to the function")
            return;
        
        if "deviceid" not in metadata:
            metadata["deviceid"] = deviceid

        if "tenant" not in metadata:
            metadata["tenant"] = tenant

        if "timestamp" not in metadata:
            metadata["timestamp"] = int(time.time() * 1000)

        return metadata

    def update_attrs(self, tenant, deviceid, attrs, metadata):
        """
            Send an attribute update request to dojot

            :type tenant: str
            :param tenant: Tenant associated to the attrs that will update
            :type deviceid: str
            :param devideid: Id of the device that sent the message
            :type attrs: dict
            :param attrs: set of attributes to update for device in dojot
            :type metadata: dict
            :param metadata: Metadata of the device
        """
        
        event = dict()

        event["metadata"] = self.check_complete_metafields(tenant, deviceid, metadata)
        event["attrs"] = attrs

        msg = json.dumps(event)

        self.messenger.publish(self.config.dojot['subjects']['device_data'], tenant, msg)

    def on(self, subject, event, callback):
        """
            Subscribes to an event from a subject

            :type subject: str
            :param subject: The subject
            :type event: str
            :param event: The event
            :type callback: function
            :param callback: The callback to be executed. It should have
                two parameters, the tenant (a string) and data (a dict).
        """

        self.messenger.on(subject, event, callback)

def main():    
    iotagent = IoTAgent()
    # iotagent.get_device("admin","3ed75c")
    met = dict()
    met["a"] = "a"
    print(iotagent.check_complete_metafields("admin", "999888", met))

if __name__=="__main__":
    main()

