import statistics
from src.report import CreateLink
from src.utils import dask


def CheckCPULoadAverage(state, dataframe, node_stats, nodes, CPULoadAverage):
    warning_cases = []
    fail_cases = []
    nodes_corecount = []
    for node1 in node_stats['nodes']:
        loadaverage_15min = node_stats['nodes'][node1]['os']['cpu']['load_average']["15m"]
        for node2 in nodes["nodes"]:
            core_count = 0
            if node_stats['nodes'][node1]['name'] == nodes["nodes"][node2]['name']:
                for thread_pool in nodes["nodes"][node2]["thread_pool"]:
                    for parameter in nodes["nodes"][node2]["thread_pool"][thread_pool]:
                        if parameter == "core":
                            core_value = nodes["nodes"][node2]["thread_pool"][thread_pool][parameter]
                            core_count += core_value
                nodes_corecount.append([nodes["nodes"][node2]["name"], core_count, loadaverage_15min])

    for node in nodes_corecount:
        if 0.7 * float(node[1]) <= float(node[2]) <= 0.8 * float(node[1]):
            warning_cases.append(node)
        elif float(node[2]) > 0.8 * float(node[1]):
            fail_cases.append(node)

    if len(warning_cases) == 0 and len(fail_cases) == 0:
        result = 'PASS'
        description = 'There is no additional CPU load. for precise usages per node, check the overview section.'
    else:
        description = f'<b>Issue:</b>\nLoad of {len(warning_cases) + len(fail_cases)} nodes exceeded the CPU\'s capacity, indicating one or more thread pools of nodes are running low.\n'
        description1 = " "
        if len(warning_cases) > 0 and len(fail_cases) == 0:
            result = 'WARNING'
        else:
            result = "FAIL"

        for node in warning_cases:
            description1 += f'Node = {node[0]}, Current core count = {node[1]}, 15 min Load average = {node[2]}, Expected load average = {round(0.7 * node[1], 2)}\n'
        for node in fail_cases:
            description1 += f'Node = {node[0]}, Current core count = {node[1]}, 15 min Load average = {node[2]}, Expected load average = {round(0.7 * node[1], 2)}\n'

        if description1.count('\n') > 10:
            description_link = CreateLink(description1, 'cpu_15minloadaverage.html')
            description += f'Please find them in the link given below.\n{description_link}<br>'
        else:
            description += f'\n\n<b>Nodes:</b>\n{description1}'
        suggestion = '\n<b>Suggestion:</b>\n1. Make sure ES process is what consuming all the CPU.\n2. Find high CPU consuming slow queries and try to tune them(check slow log report for the list of queries).\n3. Scale out more ES nodes.\n'
        description += suggestion

    state.add_result(result)
    dataframe.loc[len(dataframe)] = [CPULoadAverage, result, description]
    return dataframe


def CheckMemoryUsagePercent(state, dataframe, node_stats, MemoryUsagePercent):
    node_count = 0
    warning_cases = []
    fail_cases = []
    for node_key, node_value in node_stats['nodes'].items():
        value = node_stats['nodes'][node_key]['os']['mem']['used_percent']
        node_count += 1
        if value > 90:
            fail_cases.append([node_stats['nodes'][node_key]['name'], value])
        elif 85 < value <= 90:
            warning_cases.append([node_stats['nodes'][node_key]['name'], value])

    if len(fail_cases) == 0 and len(warning_cases) == 0:
        result = 'PASS'
        description = f'Total Nodes = {node_count}. Memory consumption of all nodes is < 85%. Check overall stats section for detailed usage.'
    else:
        if len(fail_cases) > 0:
            result = 'FAIL'
        else:
            result = 'WARNING'
        cases = sorted(warning_cases + fail_cases, key=lambda l: l[1], reverse=True)
        description = f"<b>Issue:</b>\nTotal nodes = {node_count}. Memory consumption of {len(cases)} nodes is > 85%.\n"
        description1 = " "
        for node in cases:
            description1 += f"Node = {node[0]}, Memoryused = {node[1]}%\n"
        if description1.count('\n') > 10:
            description_link = CreateLink(description1, 'memory_used.html')
            description += f'Please find them in the link given below.\n{description_link}<br>'
        else:
            description += f'\n\n<b>Nodes:</b>\n{description1}'
        suggestion = '\n<b>Suggestion: </b>\nLogin to the specific ES VM and use top command to check below items.\n1. Is ES process itself is consuming all the memory or any other process.\n2. If ES process memory is high and growing then it can be a native memory leak. Needs further debugging.'
        description += suggestion

    state.add_result(result)
    dataframe.loc[len(dataframe)] = [MemoryUsagePercent, result, description]
    return dataframe


def CheckDiskUsageLimits(state, dataframe, cat_allocation, Disk_Limits):
    fail_cases = []
    warning_cases = []
    for node in cat_allocation:
        if node['disk.percent'] is not None:
            if int(node['disk.percent']) > 90:
                fail_cases.append([node["node"], node['disk.percent']])
            elif 80 <= int(node['disk.percent']) <= 90:
                warning_cases.append([node["node"], node['disk.percent']])

    if len(fail_cases) == 0 and len(warning_cases) == 0:
        result = 'PASS'
        description = "All nodes have < 80% disk usage."
    else:
        description = f"<b>Issue:</b>\n{len(fail_cases) + len(warning_cases)} nodes have > 80% disk usage.\n"
        description1 = " "
        cases = sorted(warning_cases + fail_cases, key=lambda l: l[1], reverse=True)
        if len(fail_cases) == 0 and len(warning_cases) > 0:
            result = 'WARNING'
        else:
            result = 'FAIL'
        for node in cases:
            description1 += f'Node = {node[0]}, Disk percent = {node[1]}%\n'
        if description1.count('\n') > 10:
            description_link = CreateLink(description1, 'disk_percent.html')
            description += f'Please find them in the link given below.\n{description_link}<br>'
        else:
            description += f'\n<b>Nodes:</b>\n{description1}'
        suggestion = '\n<b>Suggestion: </b>\n1. Find any unwanted indices and delete them.\n2. Scale the disk if current disk allocation < 2tb.\n3. If we have crossed 2tb per node limit then scale out more nodes.\n'
        description += suggestion

    state.add_result(result)
    dataframe.loc[len(dataframe)] = [Disk_Limits, result, description]
    return dataframe


def CheckDiskUsageDistribution(state, dataframe, cat_allocation, Disk_Usage):
    nodes_disk_used = []
    for node in cat_allocation:
        if node['disk.used'] is not None:
            nodes_disk_used.append([node['node'], dask.utils.parse_bytes(node['disk.used'])])

    try:
        derived_standard_deviation = statistics.stdev([row[1] / (1024**3) for row in nodes_disk_used])
    except statistics.StatisticsError:
        derived_standard_deviation = 0

    acceptable_standard_deviation = 5
    if derived_standard_deviation <= acceptable_standard_deviation:
        result = 'PASS'
        description = "All nodes have good distirbution of disk usage. Check Overall stats section for detailed usage info."
    else:
        result = 'FAIL'
        description1 = " "
        description = f"<b>Issue:</b>\n Some nodes don't have a good distribution of disk usage. Observed standard deviation of {derived_standard_deviation}. \nCheck overall stats section to find detailed per node usage.\n"
        for node in nodes_disk_used:
            description1 += f"Node = {node[0]}, Disk used = {round(node[1] / (1024**3))}GB \n"
        if description1.count('\n') > 10:
            description_link = CreateLink(description1, 'disk_usage_distribution.html')
            description += f'Please find them in the link given below.\n{description_link}<br>'
        else:
            description += f'\n<b>Nodes:</b>\n{description1}'
        suggestion = '\n <b>Suggestion:</b>\nCheck if all the shards distribution is proper.\n'
        description += suggestion

    state.add_result(result)
    dataframe.loc[len(dataframe)] = [Disk_Usage, result, description]
    return dataframe


def CheckSwap(state, dataframe, nodes_stats, SwapDisabled):
    swap_enabled_nodes = []
    for node in nodes_stats["nodes"]:
        swap_space = nodes_stats["nodes"][node]["os"]["swap"]["total_in_bytes"]
        if swap_space != 0:
            swap_enabled_nodes.append([nodes_stats["nodes"][node]["name"], swap_space])

    if len(swap_enabled_nodes) == 0:
        result = 'PASS'
        description = 'Swap disabled for all nodes.'
    else:
        result = 'FAIL'
        description = f'<b>Issue:</b>\nSwap enabled for below nodes.\n'
        description1 = " "
        for node in swap_enabled_nodes:
            description1 += f'Node = {node[0]}, Swap space = {node[1]}\n'
        if description1.count('\n') > 10:
            description_link = CreateLink(description1, 'swap.html')
            description += f'Please find them in the link given below.\n{description_link}<br>'
        else:
            description += f'\n<b>Nodes:</b>\n{description1}'
        suggestion = '\n<b>Suggestion:</b>\nSwap usage impacts the performance. Disable the swap using "/sbin/sysctl -w vm.swappiness=0" \n'
        description += suggestion

    state.add_result(result)
    dataframe.loc[len(dataframe)] = [SwapDisabled, result, description]
    return dataframe
