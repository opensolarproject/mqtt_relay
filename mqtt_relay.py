#! /usr/bin/env python
import os, time, csv, argparse, subprocess
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
  influx_measurement = "mqtt_data"

  def run(self, instr, outstr, keys, influx_config):
    self.sub = self.clientFromStr(instr, subscribe=True)
    self.pub = self.clientFromStr(outstr, subscribe=False)
    for key in keys:
      self.forwardVars[key] = 0

    if influx_config:
      self.setup_influxdb(influx_config)

    print("using forwardVars", self.forwardVars)
    self.sub.loop_start()
    self.pub.loop_start()
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
    self.influx_measurement = config['measurement']
    print(Back.GREEN + Fore.BLACK + "Connected to InfluxDB" + Style.RESET_ALL)

  def runCSVOutputLoop(self):
    try:
      lastChangeover = time.strftime(self.logRotateFormat)
      fname = os.path.join(self.csvdir, time.strftime(self.logRotateFormat) + "_mppt.csv")  # Use csvdir
      didexist = os.path.isfile(fname)
      with open(fname, 'a', newline='') as outfile:
        print(Back.YELLOW + Fore.BLACK + "opening CSV file " + fname + Style.RESET_ALL)
        keys = ['time','unix']
        keys.extend(self.allValues.keys()) #operates in-place it seems
        writer = csv.DictWriter(outfile, delimiter=',', fieldnames=keys)
        if not didexist: writer.writeheader()
        else: print("appending existing csv!")
        while True:
          self.recentValues['time'] = time.strftime("%Y%m%dT%H%M%S%Z")
          self.recentValues['unix'] = time.time()
          writer.writerow(self.recentValues)
          print(Back.BLUE + Fore.BLACK + time.strftime("%Y%m%dT%H%M%S%Z") + Style.RESET_ALL + " " + str(self.recentValues))
          self.forward_to_influxdb(self.recentValues)
          self.recentValues = {} #clear old ones
          time.sleep(self.csvPeriod)
          if lastChangeover != time.strftime(self.logRotateFormat):
            return
    finally:
      print("\n" + "closing " + fname)
      outfile.close()
      subprocess.check_call(['gzip', fname])
      print("gzipped " + fname)

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
    self.pub.loop_stop()

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
      # else: print(key, val, Fore.YELLOW + "HELD" + Style.RESET_ALL)
    # else: print(key, val, Fore.RED + "muggle" + Style.RESET_ALL)

  def sendVar(self, key, val):
     self.pub.publish(self.pubPrefix + key, val)
     return

  def clientFromStr(self, s, subscribe):
    proto_split = splitCheck(s,'://')
    at_split = splitCheck(proto_split[1], '@')
    sub_split = splitCheck(at_split[1], '/', False)
    user_split = splitCheck(at_split[0], ':', False)
    ret = mqtt.Client(self.clientname)
    ret.username_pw_set(user_split[0], user_split[1])
    ret.on_connect=self.connected
    ret.on_message=self.msg

    host_split = splitCheck(sub_split[0], ":", False)
    port = int(host_split[1]) if len(host_split) == 2 else (8883 if proto_split == "mqtts" else 1883)
    if port == 8883: ret.tls_set()
    ret.connect(host_split[0], port)
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
  parser = argparse.ArgumentParser(description="MQTT-Relay")
  parser.add_argument('-i','--in', required=True, help="form: mqtt[s]://user:pass@host[:port]/topic/#")
  parser.add_argument('-o','--out', required=True, help="form: mqtt[s]://user:pass@host[:port]/topicprefix")
  parser.add_argument('-p','--period', type=int, default=relay.period, help="interval between forwardings")
  parser.add_argument('-l','--lazyPeriod', type=int, default=relay.lazyPeriod, help="interval when inactive")
  parser.add_argument('-c','--csvPeriod', type=int, default=relay.csvPeriod, help="csv interval")
  parser.add_argument('--csvdir', default="./csvs", help="directory to write csv files to")
  parser.add_argument('--lazyKey', default=relay.lazyKey, help="enable key to come out of lazy-mode")
  parser.add_argument('--name', default=relay.clientname, help="name to connect to MQTT dbs with")
  parser.add_argument('-k','--key', nargs='*', type=str, help="keys to forward to output mqtt")
  parser.add_argument('--keys', type=str, help="keys to forward to mqtt, comma seperated")
  parser.add_argument('--influx-host', help="InfluxDB host")
  parser.add_argument('--influx-port', type=int, default=8086, help="InfluxDB port")
  parser.add_argument('--influx-user', help="InfluxDB username")
  parser.add_argument('--influx-password', help="InfluxDB password")
  parser.add_argument('--influx-database', help="InfluxDB database name")
  parser.add_argument('--influx-measurement', default=relay.influx_measurement, help="InfluxDB measurement name")
  parser.add_argument('-x', '--extra-sub', nargs='*', type=str, help="extra subscriptions to make / forward to influx")
  args = parser.parse_args()

  if args.keys is not None: args.key = args.keys.split(',')
  relay.period = args.period
  relay.csvdir = args.csvdir
  relay.lazyPeriod = args.lazyPeriod
  relay.clientname = args.name

  influx_config = None
  if args.influx_host and args.influx_database:
    influx_config = {
      'host': args.influx_host,
      'port': args.influx_port,
      'user': args.influx_user,
      'password': args.influx_password,
      'database': args.influx_database,
      'measurement': args.influx_measurement
    }

  relay.run(getattr(args, 'in'), args.out, args.key, influx_config)

if __name__ == '__main__':
  main()
