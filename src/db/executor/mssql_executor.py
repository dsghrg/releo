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
        start_time = time.time()
        cursor.execute(sql_query)
        elapsed_time = 1000 * (time.time() - start_time)
        cursor.nextset()
        res_two = cursor.fetchall()
        xml_plan = res_two[0][0]
        # filename = "query_execution-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(time.time())) + ".xml"
        # f = open('./' + filename, 'w+')
        # f.write(xml_plan)
        # f.close()
        parsed = ET.ElementTree(ET.fromstring(xml_plan))
        root = {'children': [], 'isRoot': True, 'cost': elapsed_time}
        traverse(parsed.getroot(), self.schema, root)
        return root


def traverse(current, schema, parent):
    for child in current.getchildren():
        parent = traverse(child, schema, parent)

    if "relop" in current.tag.lower() and 'LogicalOp' in current.attrib and 'join' in current.attrib[
        'LogicalOp'].lower():
        runtime = current.findall('./mw:RunTimeInformation', namespaces=namespace)[0]
        join_info = \
            [node for node in current.getchildren() if
             node.find('./mw:HashKeysBuild', namespaces=namespace) is not None][0]
        cost = max([float(thread.attrib['ActualElapsedms']) for thread in runtime.getchildren()])

        keyBuild = join_info.find('./mw:HashKeysBuild', namespaces=namespace)[0]
        keyProbe = join_info.find('./mw:HashKeysProbe', namespaces=namespace)[0]
        right = keyBuild.attrib['Table'].replace('[', '').replace(']', '')
        right_table = schema[right]
        left = keyProbe.attrib['Table'].replace('[', '').replace(']', '')
        left_table = schema[left]
        join = {'left': left_table.name, 'right': right_table.name, 'cost': cost, 'children': []}
        parent['children'].append(join)
        return join
    return parent
