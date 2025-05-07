#! /usr/bin/env python
import os, time, csv, datetime, toml
import paho.mqtt.client as mqtt
from colorama import Fore, Back, Style
import influxdb_client, traceback, signal

class Relay:
  clientname = "relay"
  forwardVars = dict()
  mqttFwdMinTime = 8
  lazyPeriod = 45
  csvPeriod = 8
  csvdir = None
  allValues = {}
  valuesTimes = {}
  csvValuesCohort = {}
  influxValueCohort = {}
  lazyKey = "outputEN"
  pubPrefix = ""
  logRotateFormat = "%Y%m%d"
  influx_client = None
  influx_bucket = None
  influxFwdMinTime = 2
  influxFwdLastTx = 0

  def run(self, config):
    self.lazyPeriod = config.get('lazyPeriod', self.lazyPeriod)
    self.csvPeriod = config.get('csvPeriod', self.csvPeriod)
    self.csvdir = config.get('csvdir', self.csvdir)
    self.lazyKey = config.get('lazyKey', self.lazyKey)
    self.clientname = config.get('name', self.clientname)

    ## -- setup mqtt -- ##
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
    self.mqttFwdMinTime = mqtt_config.get('min_interval', self.mqttFwdMinTime)

    ## -- setup influxdb -- ##
    influx_config = config.get('influxdb')
    if influx_config:
      self.influx_client = influxdb_client.InfluxDBClient(
        url=influx_config['url'],
        org=influx_config['org'],
        token=influx_config['token'],
      )
      self.influx_bucket = influx_config['bucket']
      self.influxFwdMinTime = influx_config.get('min_interval', self.influxFwdMinTime)
      print(Back.GREEN + Fore.BLACK + "Set InfluxDB connection" + Style.RESET_ALL, self.influx_client)

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
          self.csvValuesCohort['time'] = time.strftime("%Y%m%dT%H%M%S%Z")
          self.csvValuesCohort['unix'] = time.time()
          for key in list(self.csvValuesCohort.keys()):
            if key not in keys:
              print("removing not-included csv key", key)
              del self.csvValuesCohort[key]
          writer.writerow(self.csvValuesCohort)
          outfile.flush()
          os.fsync(outfile.fileno())
          print(Back.BLUE + Fore.BLACK + time.strftime("%Y%m%dT%H%M%S%Z") + Style.RESET_ALL + " csv write " + str(self.csvValuesCohort))
          # self.forward_to_influxdb(self.csvValuesCohort)
          self.csvValuesCohort = {} #clear old ones
          time.sleep(self.csvPeriod)
          if lastChangeover != time.strftime(self.logRotateFormat):
            return
    finally:
      print("\n" + "closing " + fname)
      outfile.close()
      # subprocess.check_call(['gzip', fname])
      # print("gzipped " + fname)

  def send_influx_cohort(self):
    if not self.influxValueCohort or not self.influx_client:
      return #nothing to send
    if (time.time() - self.influxFwdLastTx) < self.influxFwdMinTime:
      return
    ts = max(self.valuesTimes.values(), default=time.time()) # get the latest timestamp
    ts_iso = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat()

    points = []
    for key, value in self.influxValueCohort.items():
      try:
        val = float(value)
      except ValueError:
        val = value  # allow non-numeric if needed
      points.append(influxdb_client.Point(key).field("value", val).time(ts_iso))
    try:
      print(Back.GREEN + Fore.BLACK + "Forwarded to InfluxDB:" + Style.RESET_ALL, self.influxValueCohort)
      self.influx_client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS).write(bucket=self.influx_bucket, record=points)
    except Exception as e:
      print(Back.RED + Fore.BLACK + "Error sending to InfluxDB: " + str(e) + Style.RESET_ALL)
    self.influxValueCohort.clear() #Clear the cohorts after sending

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
    self.csvValuesCohort[key] = val
    self.valuesTimes[key] = time.time()
    if key in self.forwardVars:
      if (time.time() - self.forwardVars[key]) > (self.mqttFwdMinTime if self.isEnabled() else self.lazyPeriod):
        self.sendVar(key, val)
        self.forwardVars[key] = time.time()
    if self.influx_client:
      if key in self.influxValueCohort: # already in cohort? time to send old values
        self.send_influx_cohort()
        # TODO maybe set timer for next influx publish? or use csv loop?
      self.influxValueCohort[key] = val

  def sendVar(self, key, val):
    if self.pub:
      self.pub.publish(self.pubPrefix + key, val)
      print(key, val, Back.GREEN + Fore.BLACK + "sent" + Style.RESET_ALL)
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


if __name__ == '__main__':
  relay = Relay()
  config_path = "config.toml"
  if not os.path.exists(config_path):
    logme(Back.RED + Fore.BLACK + f"Config file {config_path} not found!" + Style.RESET_ALL)
    exit(1)
  def handle_sigterm(signum, frame):
    logme(Back.RED + Fore.BLACK + "SIGTERM received, shutting down..." + Style.RESET_ALL)
    relay.stop()
    exit(0)
  signal.signal(signal.SIGTERM, handle_sigterm)

  config = toml.load(config_path)
  relay.run(config)
