import xml.etree.ElementTree as ET
import time

namespace = {'mw': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}


class MssqlJoinBreakdownXml:

    def __init__(self, cfg, engine, schema):
        self.cfg = cfg
        self.engine = engine
        self.schema = schema

    def execute(self, sql_query):
        con = self.engine.raw_connection()
        cursor = con.cursor()
        cursor.execute(sql_query)
        cursor.nextset()
        res_two = cursor.fetchall()
        xml_plan = res_two[0][0]
        filename = "query_execution-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(time.time())) + ".xml"
        f = open('./' + filename, 'w+')
        f.write(xml_plan)
        f.close()
        parsed = ET.ElementTree(ET.fromstring(xml_plan))
        costs = {}
        for elem in parsed.iter():
            traverse(elem, costs, self.schema, '')
        return costs


def traverse(current, costs, schema, path):
    if "relop" in current.tag.lower() and 'LogicalOp' in current.attrib and 'join' in current.attrib[
        'LogicalOp'].lower():

        runtime = current.findall('./mw:RunTimeInformation', namespaces=namespace)[0]
        join_info = \
            [node for node in current.getchildren() if
             node.find('./mw:HashKeysBuild', namespaces=namespace) is not None][0]
        cost = max([float(thread.attrib['ActualElapsedms']) for thread in runtime.getchildren()])

        keyBuild = join_info.find('./mw:HashKeysBuild', namespaces=namespace)[0]
        keyProbe = join_info.find('./mw:HashKeysProbe', namespaces=namespace)[0]
        left = keyBuild.attrib['Table'].replace('[', '').replace(']', '')
        left_table = schema[left]
        right = keyProbe.attrib['Table'].replace('[', '').replace(']', '')
        right_table = schema[right]
        path = path + right_table.name if path != '' else left_table.name + '->' + right_table.name
        costs[path] = cost
        for child in current.getchildren():
            traverse(child, costs, schema, path)
