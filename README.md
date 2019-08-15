# The Open Solar Project _MQTT Relay_

Used to forward real-time MQTT datapoints to another MQTT database. For example I use it to forward to Adafruit.io for convienient visualization.

### Head over to the [Data & Visualization](https://github.com/opensolarproject/OSPController/wiki/Step-4%3A-Data-Visualization) page on the [OSP wiki](https://github.com/opensolarproject/OSPController/wiki) for background, setup, etc.


| ![chart example](https://github.com/opensolarproject/OSPController/wiki/images/charts2.png) |
| --- |
| _here's a shot of my adafruit dashboard_ |


## Setup
- `python3 -m pip install paho-mqtt colorama`
- `python3 mqtt_relay.py --help`
- Run in a screen: `screen -AdmS relay python3 mqtt_relay.py --...`
- Go see more complete setup on [the wiki](https://github.com/opensolarproject/OSPController/wiki/Step-4%3A-Data-Visualization)
