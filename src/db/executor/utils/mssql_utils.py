namespace = {'mw': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}


def traverse(current, schema, parent):
    for child in current.getchildren():
        parent = traverse(child, schema, parent)

    if "relop" in current.tag.lower() and 'LogicalOp' in current.attrib and 'join' in current.attrib[
        'LogicalOp'].lower():
        runtime = current.findall('./mw:RunTimeInformation', namespaces=namespace)[0]
        cost = max([float(thread.attrib['ActualElapsedms']) for thread in runtime.getchildren()])

        join = {}
        if current.attrib['PhysicalOp'] == 'Merge Join':
            join = _merge_join(current, schema)
        elif current.attrib['PhysicalOp'] == 'Hash Match':
            join = _hash_join(current, schema)
        elif current.attrib['PhysicalOp'] == 'Nested Loops':
            join = _nested_loop(current, schema)
        else:
            print('unknonwn join ' + str(current.attrib['PhysicalOp']))

        join['cost'] = cost
        parent['children'].append(join)
        return join
    return parent


def _hash_join(current, schema):
    join_info = \
        [node for node in current.getchildren() if
         node.find('./mw:HashKeysBuild', namespaces=namespace) is not None][0]

    keyBuild = join_info.find('./mw:HashKeysBuild', namespaces=namespace)[0]
    keyProbe = join_info.find('./mw:HashKeysProbe', namespaces=namespace)[0]
    right = keyBuild.attrib['Table'].replace('[', '').replace(']', '')
    right_table = schema[right]
    left = keyProbe.attrib['Table'].replace('[', '').replace(']', '')
    left_table = schema[left]
    return {'left': left_table.name, 'right': right_table.name, 'children': []}


def _merge_join(current, schema):
    join_info = current.find('./mw:Merge', namespaces=namespace)
    innerSide = join_info.find('./mw:InnerSideJoinColumns', namespaces=namespace).getchildren()[0]
    outerSide = join_info.find('./mw:OuterSideJoinColumns', namespaces=namespace).getchildren()[0]
    right = innerSide.attrib['Table'].replace('[', '').replace(']', '')
    right_table = schema[right]
    left = outerSide.attrib['Table'].replace('[', '').replace(']', '')
    left_table = schema[left]
    return {'left': left_table.name, 'right': right_table.name, 'children': []}


def _nested_loop(current, schema):
    join_info = current.find('./mw:NestedLoops', namespaces=namespace)
    join_info = join_info.find('./mw:Predicate', namespaces=namespace)
    join_info = join_info.find('./mw:ScalarOperator', namespaces=namespace)
    join_info = join_info.find('./mw:Compare', namespaces=namespace)

    left_side = join_info.findall('./mw:ScalarOperator', namespaces=namespace)[0].getchildren()[0].getchildren()[0]
    right_side = join_info.findall('./mw:ScalarOperator', namespaces=namespace)[1].getchildren()[0].getchildren()[0]
    right = right_side.attrib['Table'].replace('[', '').replace(']', '')
    right_table = schema[right]
    left = left_side.attrib['Table'].replace('[', '').replace(']', '')
    left_table = schema[left]
    return {'left': left_table.name, 'right': right_table.name, 'children': []}
