#! /usr/bin/env python
import os, time, csv, subprocess, toml
import paho.mqtt.client as mqtt
from colorama import Fore, Back, Style
from influxdb import InfluxDBClient

class Relay:
  clientname = "relay"
  forwardVars = dict()
  period = 12
  lazyPeriod = 45
  csvPeriod = 8
  csvdir = None
  allValues = {}
  recentValues = {}
  lazyKey = "outputEN"
  pubPrefix = ""
  logRotateFormat = "%Y%m%d"
  influx_client = None

  def run(self, config):
    self.period = config.get('period', self.period)
    self.lazyPeriod = config.get('lazyPeriod', self.lazyPeriod)
    self.csvPeriod = config.get('csvPeriod', self.csvPeriod)
    self.csvdir = config.get('csvdir', self.csvdir)
    self.lazyKey = config.get('lazyKey', self.lazyKey)
    self.clientname = config.get('name', self.clientname)

    influx_config = config.get('influxdb')
    if influx_config:
      self.setup_influxdb(influx_config)

    mqtt_config = config.get('mqtt', {})
    if not mqtt_config:
      return print(Back.RED + Fore.BLACK + "No MQTT configuration config found!" + Style.RESET_ALL)
    if not mqtt_config.get('input'):
      return print(Back.RED + Fore.BLACK + "No MQTT input configuration found!" + Style.RESET_ALL)
    self.sub = self.clientFromStr(mqtt_config.get('input'), subscribe=True)
    self.pub = self.clientFromStr(mqtt_config.get('output'), subscribe=False) if mqtt_config.get('output') else None
    keys = mqtt_config.get('keys', [])
    if not keys: return print(Back.RED + Fore.BLACK + "No keys to forward!" + Style.RESET_ALL)
    for key in keys:
      self.forwardVars[key] = 0

    print("using forwardVars", self.forwardVars)
    self.sub.loop_start()
    self.pub.loop_start() if self.pub else None
    try:
      time.sleep(2)
      while True:
        if self.csvdir:
          self.runCSVOutputLoop()
        else:
          time.sleep(1)  # Keep the MQTT thread running
    except KeyboardInterrupt:
      print(Back.RED + Fore.BLACK + "now exiting" + Style.RESET_ALL)

    self.stop()

  def setup_influxdb(self, config):
    self.influx_client = InfluxDBClient(
      host=config['host'],
      port=config['port'],
      username=config['user'],
      password=config['password'],
      database=config['database']
    )
    print(Back.GREEN + Fore.BLACK + "Set InfluxDB connection" + Style.RESET_ALL, self.influx_client)

  def runCSVOutputLoop(self):
    try:
      lastChangeover = time.strftime(self.logRotateFormat)
      fname = os.path.join(self.csvdir, time.strftime(self.logRotateFormat) + "_mppt.csv")  # Use csvdir
      if not os.path.exists(self.csvdir):
        os.makedirs(self.csvdir)
      didexist = os.path.isfile(fname)
      with open(fname, 'a', newline='') as outfile:
        print(Back.YELLOW + Fore.BLACK + "opening CSV file " + fname + Style.RESET_ALL)
        keys = ['time','unix']
        keys.extend(self.allValues.keys()) #operates in-place it seems
        keys.extend(self.forwardVars.keys())
        print("csv keys", keys)
        writer = csv.DictWriter(outfile, delimiter=',', fieldnames=keys)
        if not didexist: writer.writeheader()
        else: print("appending existing csv!")
        while True:
          self.recentValues['time'] = time.strftime("%Y%m%dT%H%M%S%Z")
          self.recentValues['unix'] = time.time()
          writer.writerow(self.recentValues)
          outfile.flush()
          os.fsync(outfile.fileno())
          print(Back.BLUE + Fore.BLACK + time.strftime("%Y%m%dT%H%M%S%Z") + Style.RESET_ALL + " " + str(self.recentValues))
          # self.forward_to_influxdb(self.recentValues)
          self.recentValues = {} #clear old ones
          time.sleep(self.csvPeriod)
          if lastChangeover != time.strftime(self.logRotateFormat):
            return
    finally:
      print("\n" + "closing " + fname)
      outfile.close()
      # subprocess.check_call(['gzip', fname])
      # print("gzipped " + fname)

  def forward_to_influxdb(self, data):
    if self.influx_client:
      json_body = [
        {
          "measurement": self.influx_measurement,
          "time": data['time'],
          "fields": {k: float(v) if v.replace('.', '', 1).isdigit() else v for k, v in data.items() if k not in ['time', 'unix']}
        }
      ]
      self.influx_client.write_points(json_body)
      print(Back.GREEN + Fore.BLACK + "Forwarded to InfluxDB" + Style.RESET_ALL)

  def stop(self):
    self.sub.loop_stop()
    self.pub.loop_stop() if self.pub else None

  def connected(self, client, userdata, flags, rc):
    print(Back.YELLOW + Fore.BLACK + "Connected to broker " + tostr(client) + Style.RESET_ALL)

  def isEnabled(self):
    return (self.allValues[self.lazyKey] != '0') if self.lazyKey in self.allValues else False

  def msg(self, client, userdata, message):
    key = message.topic.split("/")[-1]
    val = str(message.payload.decode("utf-8"))
    self.allValues[key] = val
    self.recentValues[key] = val
    if key in self.forwardVars:
      if (time.time() - self.forwardVars[key]) > (self.period if self.isEnabled() else self.lazyPeriod):
        self.sendVar(key, val)
        print(key, val, Back.GREEN + Fore.BLACK + "sent" + Style.RESET_ALL)
        self.forwardVars[key] = time.time()
    # TODO set timer for next influx publish
    self.forward_to_influxdb(self.recentValues)

  def sendVar(self, key, val):
     if self.pub:
       self.pub.publish(self.pubPrefix + key, val)
     return

  def clientFromStr(self, s, subscribe):
    proto_split = splitCheck(s,'://')
    at_split = splitCheck(proto_split[1], '@')
    sub_split = splitCheck(at_split[1], '/', False)
    user_split = splitCheck(at_split[0], ':', False)
    ret = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, self.clientname)
    ret.username_pw_set(user_split[0], user_split[1])
    ret.on_connect = lambda client, userdata, flags, rc: self.connected(client, userdata, flags, rc)
    ret.on_message = lambda client, userdata, message: self.msg(client, userdata, message)

    host_split = splitCheck(sub_split[0], ":", False)
    port = int(host_split[1]) if len(host_split) == 2 else (8883 if proto_split == "mqtts" else 1883)
    if port == 8883: ret.tls_set()
    try:
      ret.connect(host_split[0], port)
    except ConnectionRefusedError:
      print(Back.RED + Fore.BLACK + f"Connection to {host_split[0]}:{port} refused!" + Style.RESET_ALL)
      exit(1)
    if len(sub_split) > 1:
      if subscribe:
        print("subscribing " + tostr(ret) + " to " + sub_split[1])
        ret.subscribe(sub_split[1])
      else: self.pubPrefix = sub_split[1]
    return ret

def splitCheck(s, delim, throws=True):
  split = s.split(delim, 1)
  if len(split) != 2 and throws:
    raise ValueError("expecting <value>"+delim+"<value2")
  return split

def tostr(client):
  return str(client._username.decode("utf-8")) + "@" + str(client._host) + ":" + str(client._port)


def main():
  relay = Relay()
  config_path = "config.toml"
  if not os.path.exists(config_path):
    return print(Back.RED + Fore.BLACK + f"Config file {config_path} not found!" + Style.RESET_ALL)

  config = toml.load(config_path)
  relay.run(config)

if __name__ == '__main__':
  main()
