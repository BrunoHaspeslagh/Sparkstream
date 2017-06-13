from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import xml.etree.ElementTree as ET
from neo4j.v1 import GraphDatabase, basic_auth


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




"""driver = GraphDatabase.driver("bolt://pint-n2:7687", auth=basic_auth("neo4j","Swh^bdl"), encrypted=False)

def writeline(line):
    v = line.id
    session = driver.session()
    session.run("CREATE (n:Node {value: {v} })", {'v': v})
    session.close()
    return "CREATE {n:Node {value: {v}})"


sc = SparkContext(appName="Streaming")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("172.23.80.245", 5580).flatMap(lambda xml: XmlParser.parsefullxml(xml))
edges = lines.map(lambda edge:"aantal wagens: "+ countvehicles(edge))

res = edges.map(lambda edge: writeline(edge))

res.pprint()
#edges.pprint()
ssc.start()
ssc.awaitTermination()"""


sc = SparkContext()
batch = []
max = None
processed = 0

def writeBatch(b):
    print("writing batch of " + str(len(b)))
    session = driver.session()
    session.run('UNWIND {batch} AS elt CREATE (n:Node {v: elt})', {'batch': b})
    session.close()

def write2neo(v):
    batch.append(v)
    global processed
    processed += 1
    if len(batch) >= 500 or processed >= max:
        writeBatch(batch)
        batch[:] = []

dt = sc.parallelize(range(1, 2136))
max = dt.count()
dt.foreach(write2neo)