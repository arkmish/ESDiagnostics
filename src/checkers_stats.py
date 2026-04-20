import pandas as pd
import statistics
from src.utils import highlight, red_color, dask


def ClusterConfiguration(nodes, cat_nodes):
    nodes_count = nodes["_nodes"]["total"]
    df = pd.DataFrame(columns=['Configuration'])
    row_header = ["Basic Configuration", "Build flavor", "Roles", "Current Master", "OS and JVM",
                  "OS Name", "Version", "Arch", "Processors", "JVM Version", "Heap size min", "Heap size max"]
    for row_index in row_header:
        df.loc[len(df)] = row_index
    for node in nodes["nodes"]:
        blank = " "
        name = nodes["nodes"][node]["name"]
        build_flavor = nodes["nodes"][node]["build_flavor"]
        roles = " , ".join(nodes["nodes"][node]["roles"])
        master = "No"
        for cat_node in cat_nodes:
            if cat_node["name"] == name:
                if cat_node["master"] == "*":
                    master = 'Yes'
                break
        os_name = nodes["nodes"][node]["os"]["name"]
        os_version = nodes["nodes"][node]["os"]["version"]
        os_arch = nodes["nodes"][node]["os"]["arch"]
        os_processors = nodes["nodes"][node]["os"]["available_processors"]
        jvm_version = nodes["nodes"][node]["jvm"]["version"]
        jvm_heap_min = str(int((nodes["nodes"][node]["jvm"]["mem"]["heap_init_in_bytes"]) / (1024 ** 3))) + 'GB'
        jvm_heap_max = str(int((nodes["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"]) / (1024 ** 3))) + 'GB'

        df[name] = [blank, build_flavor, roles, master, blank, os_name, os_version, os_arch, os_processors, jvm_version, jvm_heap_min, jvm_heap_max]

    styles = [
        dict(selector='', props=[("text-align", "center"), ('border', '2px solid blue'), ('background-color', 'white'), ('border-color', 'black')]),
        dict(selector='th', props=[('font-size', '12px'), ('border-style', 'solid'), ('height', "30px"), ('border-width', '0px'),
             ('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'), ("font-weight", "normal"), ('vertical-align', 'center')]),
        dict(selector="tbody td", props=[("border", "1px solid grey"), ('font-size', '12px'), ('border-width', '0.5px')])
    ]

    df = df.style.set_table_styles(styles).set_properties(subset=df.columns, **{'width': '100px'}).hide(axis="index")
    dfnew = df.apply(highlight, axis=1)
    return dfnew


def OverallStats(nodes_stats, cat_allocation):
    disk_data = []
    df = pd.DataFrame(columns=['Configuration'])
    row_list = ["Store size", "CPU 15min Load Average", "Memory Usage", "swap", "Disk Usage",
                "Threadpool Usage", "search", "write", "get", "refresh", "snapshot", "management"]
    for row_index in row_list:
        df.loc[len(df)] = row_index
    for node in nodes_stats["nodes"]:
        blank = " "
        name = nodes_stats["nodes"][node]["name"]
        store_size = str(int((nodes_stats["nodes"][node]["indices"]["store"]["size_in_bytes"]) / (1024 ** 3))) + 'GB'
        cpu_15minloadaverage = "{:0.2f}".format(nodes_stats["nodes"][node]["os"]["cpu"]["load_average"]["15m"])
        memory_total = str(int((nodes_stats["nodes"][node]["os"]["mem"]["total_in_bytes"]) / (1024 ** 3))) + 'GB'
        memory_usage = str(int((nodes_stats["nodes"][node]["os"]["mem"]["used_in_bytes"]) / (1024 ** 3))) + 'GB'
        memory = memory_usage + "/" + memory_total
        swap = nodes_stats["nodes"][node]["os"]["swap"]["total_in_bytes"]
        disk_used = ""
        disk_total = ""
        for cat_node in cat_allocation:
            if cat_node["node"] == name:
                disk_used = str(cat_node.get("disk.used", ""))
                disk_total = str(cat_node.get("disk.total", ""))
                if disk_used and disk_total:
                    disk_data.append([cat_node["node"], disk_used.upper(), disk_total.upper()])
                break
        disk = disk_used.upper() + "/" + disk_total.upper() if disk_used and disk_total else "/"
        thread_pool_list = ["search", "write", "get", "refresh", "snapshot", "management"]
        column_data = [store_size, cpu_15minloadaverage, memory, swap, disk, blank]
        for thread_pool in thread_pool_list:
            threads = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["threads"]
            queue = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["queue"]
            active = nodes_stats["nodes"][node]["thread_pool"][thread_pool]["active"]
            value = f'total:{threads} / active:{active} / queue:{queue}'
            column_data.append(value)
        df[name] = column_data

    styles = [
        dict(selector='', props=[("text-align", "center"), ('background-color', 'white'),
             ('border-color', 'black'), ('border-spacing', '2px'), ('border', '1.5px solid')]),
        dict(selector='th', props=[('font-size', '12px'), ('border-style', 'solid'), ('border', '2px solid black'), ('border-width', '0.25px'), ('height',
             "25px"), ('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'), ("font-weight", "normal"), ('vertical-align', 'center')]),
        dict(selector="tbody td", props=[("border", "1px solid grey"), ('font-size', '12px'), ('border-width', '0.25px')])
    ]

    df = df.style.set_table_styles(styles).set_properties(subset=df.columns, **{'width': '100px'}).hide(axis="index")
    dfnew = df.apply(highlight, axis=1).map(red_color)
    return dfnew, disk_data


def CheckRepeat(array, row):
    check = 0
    for i in array:
        if i[0] == row[0]:
            check = 1
            i[3] += 1
            if isinstance(i[1], int):
                i[1] = [i[1], row[1]]
            elif isinstance(i[1], list):
                i[1].append(row[1])
            break
        else:
            check = 0
    return array, check


def MaskQuery(query):
    if "\"query\":\"" in query:
        array = []
        start_idx = 0
        while True:
            index = query.find("\"query\":\"", start_idx)
            if index == -1:
                break
            end_idx = query.find("\",", index + len("\"query\":\""))
            if end_idx != -1:
                array.append(query[index:end_idx])
            start_idx = index + len("\"query\":\"")
        for i in array:
            query = query.replace(i, "\"query\":\"xxxxx")

    if "\"value\":" in query:
        array = []
        start_idx = 0
        while True:
            index = query.find("\"value\":", start_idx)
            if index == -1:
                break
            end_idx = query.find(",\"boost\":", index + len("\"value\":"))
            if end_idx != -1:
                array.append(query[index:end_idx])
            start_idx = index + len("\"value\":")
        for i in array:
            query = query.replace(i, "\"value\":xxxxx")
    return query


def SlowLog(slow_logs):
    array = []
    for log in slow_logs:
        index_name, time_in_milli, total_hits, source = "", 0, 0, ""
        msg_key = "msg" if "msg" in log else "message" if "message" in log else None
        if msg_key:
            count1, count2, count3, count4 = 0, 0, 0, 0
            for info in log[msg_key].split(",", 7):
                if "took" in info and count1 == 0:
                    index_name = info[info.index("[") + 1:info.index("]")]
                    count1 = 1
                if "took_millis" in info and count2 == 0:
                    time_in_milli = int(info[info.index("[") + 1:info.index("]")])
                    count2 = 1
                if "total_hits" in info and count3 == 0:
                    total_hits = info[info.index("[") + 1:info.index("]")]
                    if " hits" in total_hits:
                        total_hits = total_hits.split(" hits")[0]
                    count3 = 1
                if "source" in info and count4 == 0:
                    if "], id[" in info:
                        source = info[info.index("source[") + len("source["):info.index("], id[")]
                    elif "cluster.uuid" in info:
                        source = info[info.index("source[") + len("source["):info.index(", \"cluster.uuid\":")]
                    count4 = 1

        source = MaskQuery(source)
        row_list = [index_name, time_in_milli, total_hits, 1, source]
        array, check = CheckRepeat(array, row_list)
        if check == 0:
            array.append(row_list)

    for row in array:
        if isinstance(row[1], list):
            row[1] = round(statistics.mean(row[1]))

    sorted_array = sorted(array, key=lambda l: l[1], reverse=True)
    slow_log_dataframe = pd.DataFrame(sorted_array, columns=['Index Name', 'Average Total time in ms', 'Total hits', 'Number of repetitions', 'Actual Query'])
    pd.set_option("max_colwidth", 50)
    slow_log_dataframe['Actual Query'] = slow_log_dataframe['Actual Query'].str.wrap(120)

    styles = [
        dict(selector='', props=[("text-align", "center"), ('background-color', 'white'),
             ('border-color', 'black'), ('border-spacing', '2px'), ('border', '1.5px solid')]),
        dict(selector='th', props=[('font-size', '12px'), ('border-style', 'solid'), ('border', '2px solid black'), ('border-width', '0.25px'), ('height',
             "25px"), ('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'), ("font-weight", "normal"), ('vertical-align', 'left')]),
        dict(selector="tbody td", props=[("border", "1px solid grey"), ('font-size', '12px'), ('border-width', '0.25px')])
    ]

    dfnew = slow_log_dataframe.style.set_table_styles(styles).set_properties(subset=['Actual Query'], **{'text-align': 'left'}).hide(axis="index")
    return dfnew
