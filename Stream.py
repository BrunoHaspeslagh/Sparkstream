from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import xml.etree.ElementTree as ET
from neo4j.v1 import GraphDatabase, basic_auth
import neo4j.v1.security
import json

class ParkingLot(object):
    name = "Default"
    free = 0
    max = 0

    lat = 0
    long = 0

    entrances = []
    def __init__(self, naam="Default",free=0 ,max = 0 , lat = 0 , lon = 0):
        self.name = naam
        self.lat = lat
        self.long = lon
        self.max = max
        self.free = free

    @staticmethod
    def parsefromjson(jsonstring):

        j = json.loads(jsonstring)
        lots = []
        for lot in j["parkingLots"]:
            lots.append(ParkingLot(lot["name"], lot["free"]))

        return lots


class Vehicle(object):
    id = 0
    pos = 0
    speed = 0

    edge = 0
    lane = 0

    def __init__(self, id= 0, pos= 0, speed= 0, edge = 0 , lane = 0):
        self.id = id
        self.pos = pos
        self.speed = speed

        self.edge = edge
        self.lane = lane


class Node(object):
    lat = 0
    long = 0
    id = 0

    def __init__(self, lat = 0, long = 0, id = 0):
        self.lat = lat
        self.long = long
        self.id = id

    @staticmethod
    def readnodesfromxmlfile(filepath):
        #please note: not all nodes are used
        full = ET.parse(filepath).getroot()
        nodes = []
        for node in full.findall("node"):
            nodes.append(Node(node.get("lat"), node.get("lon"), node.get("id")))
        return nodes

    @staticmethod
    def findnodebyid(list, id):
        found = 0
        for node in list:
            if(node.id == id):
                found = node
                break
        return found


class Lane(object):
    id =0
    length=0
    speed=0
    Vehicles = []

    def __init__(self, id = 0, length = 0 ,  speed = 0):
        self.id = id
        self.length = length
        self.speed = speed
        self.Vehicles = []

    def addvehicle(self, vehicle):
        self.Vehicles.append(vehicle)

    def addvehicles(self, vehicles):
        for vehicle in vehicles:
            self.addvehicle(vehicle)


class Edge(object):
    id = 0
    lanes = []
    beginning = False
    end = False
    name = 0
    numlanes = 0
    type = 0

    def __init__(self, id = 0):
        self.id = id
        self.lanes = []

    def addlane(self, lane):
        self.lanes.append(lane)

    def addlanes(self, lanes):
        for lane in lanes:
            self.addlane(lane)
    def fillNodes(self, nodes, filepath):
        ids = {"to" :0, 'from': 0}
        root = ET.parse(filepath).getroot()
        for edge in root.findall("edge"):
            if(self.id == edge.get("id")):
               ids["to"] = edge.get("to")
               ids["from"] = edge.get("from")
               break

        self.beginning = Node.findnodebyid(nodes, ids["from"])
        self.end = Node.findnodebyid(nodes, ids["to"])


class XmlParser:
    @staticmethod
    def parsefullxml(xmlstring):
        root = ET.fromstring(xmlstring)
        res = []
        for edge in root.findall("edge"):
            temp = Edge(edge.get("id"))
            for lane in edge.findall("lane"):
                l = Lane(lane.get("id"))
                for vehicle in lane.findall("vehicle"):
                    v = Vehicle(vehicle.get("id"), vehicle.get("pos"), vehicle.get("speed"))
                    l.addvehicle(v)
                temp.addlane(l)
            res.append(temp)
        return res

def countvehicles(edge):
    tot = 0
    for lane in edge.lanes:
        for veh in lane.Vehicles:
            tot+=1
    return str(tot)

"""The issue is that you are calling the Spark context from within a task, which is not allowed?"""


class Weight(object):
    id = 0
    weight = 0
    def __init__(self):
        self.id = 0
        self.weight = 0

def calculateWeight(edge):
    weight = Weight()
    weight.id = edge.id
    vehCount = 0
    for lane in edge.lanes:
        for veh in lane.Vehicles:
            vehCount += 1
    #TODO: extra weight calculations go here
    weight.val = vehCount
    return weights

def writeline(line):
    id = line.id
    weight = line.weight
    driver = GraphDatabase.driver("bolt://pint-n2:7687", auth=basic_auth("neo4j", "Swh^bdl"), encrypted=False)
    session = driver.session()
    session.run("MATCH ()-[r{id: {id}}]-() SET r.weight = {weight}", {'id': id, 'weight': weight})
    session.close()
    return str(id)

def writeParking(parking):
    name = parking.name
    free = parking.free
    driver = GraphDatabase.driver("bolt://pint-n2:7687", auth=basic_auth("neo4j", "Swh^bdl"), encrypted=False)
    session = driver.session()
    session.run("MATCH(n{name: {name}}) SET n.free = {free}", {'name': name, 'free': free})
    return name

sc = SparkContext(appName="Roaddata")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("172.23.80.245", 5580).flatMap(lambda xml: XmlParser.parsefullxml(xml))\
    .map(lambda edge: calculateWeight(edge)).map(lambda weight: writeline(weight))

parkings = ssc.socketTextStream("172.23.80.245", 5581).flatMap(lambda json: ParkingLot.parsefromjson(json))\
    .map(lambda parking: writeParking(parking))
lines.pprint()
ssc.start()
ssc.awaitTermination()