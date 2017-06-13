from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import xml.etree.ElementTree as ET


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





sc = SparkContext(appName="Streaming")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("172.23.80.245", 5580)
edges = lines.flatMap(lambda xml: XmlParser.parsefullxml(xml)).map(lambda edge:"aantal wagens: "+ countvehicles(edge))

edges.pprint()
ssc.start()
ssc.awaitTermination()