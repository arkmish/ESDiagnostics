import statistics
from src.report import CreateLink

acceptable_standard_deviation = 5


def CheckStatus(state, dataframe, cluster_health, Health_check):

    status = cluster_health['status']

    if (status == "green"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster health is {}.\nCluster is in healthy status.'.format(status)
    elif (status == "red"):
        result = 'FAIL'
        state.add_result('FAIL')
        description = '<b>Issue:</b>\nCluster health is {}. \nFew nodes are down, inactive primary shards.\n'.format(status)
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to monitor the status.'
        description += suggestion
    elif (status == "yellow"):
        result = 'WARNING'
        state.add_result('WARNING')
        description = '<b>Issue:</b>\nCluster health is {}. \nInactive primary or replica shards. Cluster still in usable state. May take some time to recover to green.\n'.format(
            status)
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to monitor the status.\n'
        description += suggestion
    else:
        result = 'UNKNOWN'
        description = '<b>Issue:</b>\nCluster health is {}. Not able to fetch the status. Diagnostics tool error.\n'
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/health to get more info.'
        description += suggestion
    list_row = [Health_check, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckIsMasterRequired(state, dataframe, cat_nodes, Master_required):

    datanodes_count = 0
    masternodes_count = 0
    for node in cat_nodes:
        if (node["master"] == "*"):
            masternodes_count += 1
        if ("d" in node["node.role"]):
            datanodes_count += 1
    if (datanodes_count < 10):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Current data nodes = {} < 10. Dedicated masters are not required.'.format(datanodes_count)
    else:
        if (datanodes_count > 12):
            result = 'FAIL'
            state.add_result('FAIL')
        elif (datanodes_count >= 10 and datanodes_count <= 12):
            result = 'WARNING'
            state.add_result('WARNING')

        if (masternodes_count == 3):
            description = 'Current data nodes = {} > 10. Master nodes = {}.'.format(datanodes_count, masternodes_count)
        elif (masternodes_count > 3):
            description = 'Current data nodes = {} > 10. Master nodes = {} > 3. Dedicated masters are not required.\n'.format(
                datanodes_count, masternodes_count)
        else:
            description = '<b>Issue:</b>\nCurrent data nodes = {} > 10. Master nodes = {} < 3.\n'.format(datanodes_count, masternodes_count)
            suggestion = '\n<b>Suggestion:</b>\nTo keep the cluster active and stable, atleast 3 dedicated masters are recommended if we have > 10 data nodes.\n'
            description += suggestion
    list_row = [Master_required, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckRelo(state, dataframe, cluster_health, Relocation_count):

    relo_count = cluster_health['relocating_shards']

    if (relo_count == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = "There are no shards for relocation."
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '<b>Issue:</b>\nRelocation count = {}\nFew nodes are restarted / down which may cause shards relocation.\n'.format(relo_count)
        suggestion = '\n<b>Suggestion:</b>\nWait for cluster to stabilise.'
        description += suggestion
    list_row = [Relocation_count, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckActiveShardsPercentage(state, dataframe, cluster_health, ActiveShardsPercentage):

    active_shards_percent = cluster_health['active_shards_percent_as_number']
    description = 'Current active shards percentage is {}% \n'.format(active_shards_percent)
    if (active_shards_percent == 100.0):
        result = 'PASS'
        state.add_result('PASS')
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '<b>Issue:</b>\nCurrent active shards percentage is {}% \nShards might be in initialisation state, relocating, or failed for placement.\n'.format(
            active_shards_percent)
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cluster/allocation/explain api to find the reason.'
        description += suggestion
    list_row = [ActiveShardsPercentage, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckShardsDistribution(state, dataframe, cat_allocation, ShardsDistribution):

    shards_nodes = []
    for node in cat_allocation:
        if (node['node'] != "UNASSIGNED"):
            shards_nodes.append([node['node'], node['shards']])
    derived_standard_deviation = statistics.stdev([int(node[1]) for node in shards_nodes])
    if (derived_standard_deviation <= acceptable_standard_deviation):
        result = 'PASS'
        state.add_result('PASS')
        description = "There is a healthy distribution of shards on all nodes."
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = "<b>Issue:</b>\nBelow nodes don't have a good distribution of shards. Observed standard deviation = {}\n".format(
            derived_standard_deviation)
        description1 = " "
        for i in shards_nodes:
            description1 += "Node = {}, Shards = {} \n".format(i[0], i[1])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'shard_distribution.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Check if shard rebalance is enabled and all nodes are in healthy state.\n2. Check for disk distribution also.\n'
        description += suggestion
    list_row = [ShardsDistribution, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckTotalShardsPerNode(state, dataframe, nodes_stats, cat_allocation, TotalShardsPerNode):

    heapsize_nodes = []
    total_shards = 0
    total_heap = 0
    total_nodes = nodes_stats["_nodes"]["total"]
    for node1 in nodes_stats["nodes"]:
        for node2 in cat_allocation:
            if (nodes_stats["nodes"][node1]['name'] == node2['node']):
                heap_size_in_bytes = nodes_stats["nodes"][node1]["jvm"]["mem"]["heap_max_in_bytes"]
                total_heap += heap_size_in_bytes
                total_shards += int(node2['shards'])
                if (float(node2['shards']) >= 25*(heap_size_in_bytes)/(1024**3)):
                    heapsize_nodes.append([nodes_stats["nodes"][node1]['name'], (heap_size_in_bytes) /
                                          (1024**3), node2['shards'], 25*(heap_size_in_bytes)/(1024**3)])
    if (len(heapsize_nodes) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Total shards placed on each node didn\'t exceed the calculation threshold.'
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '<b>Issue:</b>\nTotal shards placed on below nodes exceeded the calculation threshold.\n'.format(len(heapsize_nodes))
        description1 = " "
        for node in heapsize_nodes:
            description1 += 'Node = {}, Shards = {}, Heap size = {}GB, Expected shards = {} \n'.format(node[0], node[2], round(node[1], 3), round(node[3]))
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'total_shards_per_node.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Find all empty or unused indices and delete them.\n2. If this is a test environment we can increase the limit from 25 shards per gb memory to 40 shards per gb memory.\n3. If this is a production env, scale out more nodes.\n'
        description += suggestion
    list_row = [TotalShardsPerNode, result, description]
    dataframe.loc[len(dataframe)] = list_row
    total_heap = (heap_size_in_bytes)/(1024**3)
    return dataframe, total_shards, total_nodes, total_heap


def CheckShardsPerIndex(state, dataframe, cat_indices, Shards_Per_Index):

    warning_cases = []
    fail_cases = []
    for node in cat_indices:
        if (node['status'] != "close"):
            IndexName = node['index']
            IndexSizeinGB = round((int(node['pri.store.size'])/(1024**3)), 0)
            IndexShardsCount = int(node['pri'])

            Warning_expected_Shards = round(int(node['pri.store.size'])/(35*(1024**3)), 0)
            Error_expected_Shards = round(int(node['pri.store.size'])/(50*(1024**3)), 0)

            # print(IndexName," ",IndexSizeinGB," ",IndexShardsCount," ",Warning_expected_Shards," ",Error_expected_Shards)

            if (IndexShardsCount < Error_expected_Shards):
                fail_cases.append([IndexName, IndexSizeinGB, IndexShardsCount, Error_expected_Shards])
            elif (IndexShardsCount > Error_expected_Shards and IndexShardsCount < Warning_expected_Shards):
                warning_cases.append([IndexName, IndexSizeinGB, IndexShardsCount, Warning_expected_Shards])

    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'All nodes have good shard count distribution.'
    else:
        description = "<b>Issue:</b>\nFound {} indices with wrong shards count. Please update the shards for better performance. \nIdeally each shard can be up to 35-50GB.\n".format(
            len(warning_cases)+len(fail_cases))
        description1 = " "
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = 'WARNING'
            state.add_result('WARNING')
            for node in warning_cases:
                description1 += 'Index = {}, Index Size in GB = {}, Current Shards = {}, Expected shards = {} \n'.format(node[0], node[1], node[2], node[3])
        elif (len(fail_cases) > 0):
            result = 'FAIL'
            state.add_result('FAIL')
            if (len(warning_cases) != 0):
                for node in warning_cases:
                    description1 += 'Index = {}, Index Size in GB  = {}, Current Shards = {}, Expected shards = {} \n'.format(
                        node[0], node[1], node[2], node[3])
            for node in fail_cases:
                description1 += 'Index = {}, Index Size in GB = {}, Current Shards = {}, Expected shards = {}\n'.format(node[0], node[1], node[2], node[3])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'shards_per_index.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Going more than 50GB per shard will impact the performance.\n 2. Increase the shards using _reindex api which also helps in resolving fragmentation issues.\n'
        description += suggestion
    list_row = [Shards_Per_Index, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckUnassignedShards(state, dataframe, cat_shards, UnassignedShards):

    unassigned_shards = []
    for shard in cat_shards:
        if (shard["state"] == 'UNASSIGNED'):
            if ("unassigned.reason" in shard):
                unassigned_shards.append([shard["index"], shard["unassigned.reason"]])
            else:
                unassigned_shards.append([shard["index"], "none"])
    if (len(unassigned_shards) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'All shards are assigned.'
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '<b>Issue:</b>\nBelow shards are not assigned.\n'
        description1 = " "
        for shard in unassigned_shards:
            description1 += 'Index = {}, Reason = {}\n'.format(shard[0], shard[1])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'unassigned_shards.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nuse GET /_cat/shards?h=index,shard,prirep,state,unassigned.reason,ip | grep UNASSIGNED for more info.\n'
        description += suggestion
    list_row = [UnassignedShards, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckClusterPendingTasks(state, dataframe, cluster_pending, ClusterPendingTasks):

    less_time_consuming_tasks_count = 0
    if cluster_pending["tasks"] == []:
        result = 'PASS'
        state.add_result('PASS')
        description = "There are no cluster pending tasks."
    else:
        warning_tasks = []
        fail_tasks = []
        for task in cluster_pending['tasks']:
            value = task['time_in_queue_millis']
            if (value >= 5*10E3 and value <= 10*10E3):
                warning_tasks.append([task["source"], task['priority'], value])
            elif (value > 10*10E3):
                fail_tasks.append([task["source"], task['priority'], value])
            elif (value < 5*10E3):
                less_time_consuming_tasks_count += 1
        if (len(warning_tasks) == 0 and len(fail_tasks) == 0):
            result = 'PASS'
            state.add_result('PASS')
            if (less_time_consuming_tasks_count == 0):
                description = "There are no cluster pending tasks."
            else:
                description = 'There are {} cluster pending tasks with a duration of < 5secs.'.format(less_time_consuming_tasks_count)
        else:
            description1 = " "
            if (len(warning_tasks) > 0 and len(fail_tasks) == 0):
                result = 'WARNING'
                state.add_result('WARNING')
                description = '<b>Issue:</b>\nThe following are the cluster pending tasks that have been identified with a duration of 5-10secs.\n'
                for task in warning_tasks:
                    description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0], task[1], round(task[2]/10e3, 2))
            elif (len(fail_tasks) > 0):
                result = 'FAIL'
                state.add_result('FAIL')
                description = '<b>Issue:</b>\nThe following are the cluster pending tasks that have been identified.\n'
                if len(warning_tasks) != 0:
                    for task in warning_tasks:
                        description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0], task[1], round(task[2]/10e3, 2))
                for task in fail_tasks:
                    description1 += 'Source = {0}, Priority = {1}, Runningtime = {2} sec(s) \n'.format(task[0], task[1], round(task[2]/10e3, 2))
            if (description1.count('\n') > 10):
                description_link = CreateLink(description1, 'cluster_pending_tasks.html')
                description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
            else:
                description += '\n<b>Tasks:</b>\n'+description1
            suggestion = '\n<b>Suggestion:</b>\nGet the high time consuming tasks and need to debug based on the tasks.\n'
            description += suggestion
    list_row = [ClusterPendingTasks, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckTasks(state, dataframe, _tasks, HighTimeConsumingTasks):

    if _tasks == []:
        result = 'PASS'
        state.add_result('PASS')
        description = "There are no high time consuming tasks."
    else:
        less_time_consuming_tasks_count = 0
        warning_tasks = []
        fail_tasks = []

        for node in _tasks['nodes']:
            for task in _tasks['nodes'][node]['tasks']:
                running_time = _tasks['nodes'][node]['tasks'][task]['running_time_in_nanos']
                if (running_time >= 5*10E9 and running_time <= 10*10E9):
                    warning_tasks.append([task, _tasks['nodes'][node]['tasks'][task]['node'], running_time, _tasks['nodes'][node]['tasks'][task]['action']])
                elif (running_time > 10*10E9):
                    fail_tasks.append([task, _tasks['nodes'][node]['tasks'][task]['node'], running_time, _tasks['nodes'][node]['tasks'][task]['action']])
                elif (running_time < 5*10E9):
                    less_time_consuming_tasks_count += 1
        if (len(warning_tasks) == 0 and len(fail_tasks) == 0):
            result = 'PASS'
            state.add_result('PASS')
            if (less_time_consuming_tasks_count == 0):
                description = "There are no high time consuming tasks."
            else:
                description = 'There are {} high time consuming tasks with a duration of < 5secs.'.format(less_time_consuming_tasks_count)
        else:
            description1 = " "
            if (len(warning_tasks) > 0 and len(fail_tasks) == 0):
                result = 'WARNING'
                state.add_result('WARNING')
                description = '<b>Issue:</b>\nThe following are the high time consuming tasks that have been identified with a duration of 5-10secs.\n'
                for task in warning_tasks:
                    description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3}\n'.format(task[0], task[1], round(task[2]/10e9, 2), task[3])
            else:
                result = 'FAIL'
                state.add_result('FAIL')
                description = '<b>Issue:</b>\nThe following are the high time consuming tasks that have been identified.\n'
                if (len(warning_tasks) == 0 and len(fail_tasks) > 0):
                    for task in fail_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(
                            task[0], task[1], round(task[2]/10e9, 2,), task[3])
                elif (len(warning_tasks) > 0 and len(fail_tasks) > 0):
                    for task in warning_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(
                            task[0], task[1], round(task[2]/10e9, 2), task[3])
                    for task in fail_tasks:
                        description1 += 'Task = {0}, Node = {1}, Runningtime = {2} sec(s), Action = {3} \n'.format(
                            task[0], task[1], round(task[2]/10e9, 2), task[3])
            if (description1.count('\n') > 10):
                description_link = CreateLink(description1, 'tasks.html')
                description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
            else:
                description += '\n<b>Tasks:</b>\n'+description1
            suggestion = '\n<b>Suggestion:</b>\nGet the high time consuming tasks and need to debug based on the tasks.\n'
            description += suggestion
    list_row = [HighTimeConsumingTasks, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckHeapSizeDataNodes(state, dataframe, nodes_stats, Heapsize_datanodes):

    warning_cases = []
    fail_cases = []
    for node in nodes_stats["nodes"]:
        heap_size_in_bytes = (nodes_stats["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"])
        if ((heap_size_in_bytes) > 20*(1024**3) and (heap_size_in_bytes) < 30*(1024**3)):
            warning_cases.append([nodes_stats["nodes"][node]['name'], heap_size_in_bytes/(1024**3)])
        elif (heap_size_in_bytes <= 20*(1024**3)):
            fail_cases.append([nodes_stats["nodes"][node]['name'], heap_size_in_bytes/(1024**3)])
    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'All nodes have required heap size.'
    else:
        description = "<b>Issue:</b>\nFound {} indices with heap size < 30GB.\n".format(len(warning_cases)+len(fail_cases))
        description1 = " "
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = 'WARNING'
            state.add_result('WARNING')
            for node in warning_cases:
                description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0], node[1])
        elif (len(fail_cases) > 0):
            result = 'FAIL'
            state.add_result('FAIL')
            if (len(warning_cases) != 0):
                for node in warning_cases:
                    description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0], node[1])
            for node in fail_cases:
                description1 += 'Node = {}, Heap size = {:0.3f}GB\n'.format(node[0], node[1])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'data_nodes_heap.html')
            description += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nAs per ES recommendation, 30GB heap is optimal for performance.\n'
        description += suggestion
    list_row = [Heapsize_datanodes, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckHeapSizeMasterNodes(state, dataframe, nodes_stats, cat_nodes, Heapsize_masternodes):

    warning_cases = []
    fail_cases = []
    masternodes_count = 0
    for node in cat_nodes:
        if (node["master"] == "*"):
            masternodes_count += 1
            for node1 in nodes_stats["nodes"]:
                if (nodes_stats["nodes"][node1]["name"] == node["name"]):
                    heap_size_in_bytes = nodes_stats["nodes"][node1]["jvm"]["mem"]["heap_max_in_bytes"]
                    if ((heap_size_in_bytes) >= 4*(1024**3) and (heap_size_in_bytes) < 6*(1024**3)):
                        warning_cases.append([nodes_stats["nodes"][node1]["name"], round(heap_size_in_bytes/(1024**3), 3)])
                    elif (heap_size_in_bytes < 4*(1024**3)):
                        fail_cases.append([nodes_stats["nodes"][node1]["name"], round(heap_size_in_bytes/(1024**3), 3)])
    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        if (masternodes_count == 0):
            description = 'There are no master nodes found.'
        else:
            description = 'Number of master nodes = {}. All master nodes have required heap size.'.format(masternodes_count)
    else:
        description = "<b>Issue:</b>\nTotal master nodes  = {0}.\nDedicated master node should have around 8GB to 16GB of physical memory with a heap size of 75% of the physical memory(6GB).\nfound below master nodes with heap size < 6GB.\n".format(
            masternodes_count, len(warning_cases)+len(fail_cases))
        description1 = " "
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = 'WARNING'
            state.add_result('WARNING')
            for node in warning_cases:
                description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0], node[1])
        elif (len(fail_cases) > 0):
            result = 'FAIL'
            state.add_result('FAIL')
            if (len(warning_cases) != 0):
                for node in warning_cases:
                    description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0], node[1])
            for node in fail_cases:
                description1 += 'Node = {}, Heap size = {}GB\n'.format(node[0], node[1])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'master_nodes_heap.html')
            description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nMake sure dedicated master heap setting is atleast 4-6GB\n'
        description += suggestion
    list_row = [Heapsize_masternodes, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckTotalMemoryDataNodes(state, dataframe, nodes_stats, TotalMemoryofMasterNodes):

    fail_cases = []
    for node in nodes_stats["nodes"]:
        heap_size_in_bytes = nodes_stats["nodes"][node]["jvm"]["mem"]["heap_max_in_bytes"]
        memory_size_in_bytes = nodes_stats["nodes"][node]["os"]["mem"]["total_in_bytes"]
        expected_size_in_bytes = 2*heap_size_in_bytes+2*(1024**3)
        if (memory_size_in_bytes < expected_size_in_bytes):
            fail_cases.append([nodes_stats["nodes"][node]['name'], round(heap_size_in_bytes/(1024**3), 3),
                              round(memory_size_in_bytes/(1024**3), 3), expected_size_in_bytes])

    if (len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Nodes are configured properly.'
    else:
        description = "<b>Issue:</b>\nBasically, total RAM of a data node should be twice the heap size.\nThe nodes listed below have wrong configuration.\n"
        description1 = " "
        result = 'FAIL'
        state.add_result('FAIL')
        for node in fail_cases:
            description1 += 'Node = {}, Heap size = {}GB, Current memory = {}GB, Expected memory = {}GB\n'.format(
                node[0], node[1], node[2], round(node[3]/(1024**3)))
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'total_memory_data_nodes.html')
            description1 += 'Please find them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nAs per ES documentation, ES needs atleast double the heap space as memory. Scale up the memory.\n'
        description += suggestion
    list_row = [TotalMemoryofMasterNodes, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckThreadpoolUsage(state, dataframe, node_stats, ThreadpoolUsage):

    nodes_count = []
    warning_cases = []
    fail_cases = []
    for node in node_stats['nodes']:
        threads_count = node_stats['nodes'][node]['jvm']['threads']['count']
        active_threads_count = 0
        queue = 0
        for thread_pool in node_stats["nodes"][node]["thread_pool"]:
            for parameter in node_stats["nodes"][node]["thread_pool"][thread_pool]:
                if (parameter == "active"):
                    active_count = node_stats["nodes"][node]["thread_pool"][thread_pool][parameter]
                    active_threads_count += active_count
                elif (parameter == "queue"):
                    queue += node_stats["nodes"][node]["thread_pool"][thread_pool][parameter]
        nodes_count.append([node_stats["nodes"][node]["name"], active_threads_count, threads_count, queue])

    for node in nodes_count:
        if (float(node[1]) >= 0.7*float(node[2]) and float(node[1]) <= 0.8*float(node[2])):
            warning_cases.append(node)
        elif (float(node[1]) > 0.8*float(node[2]) or node[3] > node[2]):
            fail_cases.append(node)

    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Workload of the thread pool is maintained.'
    else:
        description = '<b>Issue:</b>\nThere are {} busy thread pools found.\n'.format(len(warning_cases)+len(fail_cases))
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = "WARNING"
            state.add_result('WARNING')
        elif (len(fail_cases) > 0):
            result = "FAIL"
            state.add_result('FAIL')
        description1 = ''
        if (len(warning_cases) != 0):
            for node in warning_cases:
                description1 += 'Node = {}, Active threads = {}, Total threads = {}, Queue = {}\n'.format(node[0], node[1], node[2], node[3])
        if (len(fail_cases) != 0):
            for node in fail_cases:
                description1 += 'Node = {}, Active threads = {}, Total threads = {}, Queue = {}\n'.format(node[0], node[1], node[2], node[3])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'thread_pool.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Find the busy pool and fix the issue accordingly.\n2. May need to scale out more nodes to support the load.\n'
        description += suggestion
    list_row = [ThreadpoolUsage, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckSegmentFragmentation(state, dataframe, cat_indices, SegmentFragmentationLevel):

    warning_cases = []
    fail_cases = []
    for index in cat_indices:
        if (index['status'] != "close"):
            total_docs = int(index["docs.count"])
            deleted_docs = int(index["docs.deleted"])
            if (total_docs != 0):
                level = (deleted_docs/(total_docs+deleted_docs))*100
                if (level >= 40 and level <= 50):
                    warning_cases.append([index["index"], deleted_docs, total_docs, level])
                elif (level > 50):
                    fail_cases.append([index["index"], deleted_docs, total_docs, level])
    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Segment fragmentation level is maintained for all indices.'
    else:
        cases = warning_cases+fail_cases
        description = '<b>Issue:</b>\nThere are {} indices with high segment fragmentation level. \n'.format(len(cases))
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = "WARNING"
            state.add_result('WARNING')
        elif (len(fail_cases) > 0):
            result = "FAIL"
            state.add_result('FAIL')
        description1 = ' '
        cases = sorted(cases, key=lambda l: l[3], reverse=True)
        for node in cases:
            description1 += 'Index = {}, Deleted docs = {}, Total_docs = {}, Percent = {}%\n'.format(node[0], node[1], node[2], round(node[3]))
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'segments_fragmentation.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nUse _reindex api to create a fresh index.\n'
        description += suggestion
    list_row = [SegmentFragmentationLevel, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckEmptyIndices(state, dataframe, cat_indices, EmptyIndices):

    empty_index = []
    for index in cat_indices:
        if (index['docs.count'] == '0'):
            empty_index.append(index['index'])
    if (len(empty_index) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'There are no empty indices in the cluster.'
    else:
        description = '<b>Issue:</b>\nThere are {} empty indices found.\n'.format(len(empty_index))
        if (len(empty_index) <= 5):
            result = 'WARNING'
            state.add_result('WARNING')
        elif (len(empty_index) > 5):
            result = 'FAIL'
            state.add_result('FAIL')
        empty_index = sorted(empty_index)
        description1 = " "
        for index in empty_index:
            description1 += 'Index = {}\n'.format(index)
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'empty_index.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Indices:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\n1. Delete all the unwanted empty indices.\n2. Every index takes few resources and also we hit max shards limit and we end up in unwanted nodes scale out.\n'
        description += suggestion
    list_row = [EmptyIndices, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckOpenFileDescriptors(state, dataframe, nodes_stats, OpenFileDescriptors):

    warning_cases = []
    fail_cases = []
    for node in nodes_stats["nodes"]:
        open_file_descriptors = nodes_stats["nodes"][node]["process"]["open_file_descriptors"]
        max_file_descriptors = nodes_stats["nodes"][node]["process"]["max_file_descriptors"]
        percentage = (open_file_descriptors/max_file_descriptors)*100
        if (percentage >= 30 and percentage <= 50):
            warning_cases.append([nodes_stats["nodes"][node]["name"], open_file_descriptors, max_file_descriptors, round(percentage)])
        elif (percentage > 50):
            fail_cases.append([nodes_stats["nodes"][node]["name"], open_file_descriptors, max_file_descriptors, round(percentage)])
    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Open file decriptors are maintained.'
    else:
        description = '<b>Issue:</b>\nOpen file descriptors of some nodes are exceeding their limit.\n'
        description1 = " "
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = "WARNING"
            state.add_result('WARNING')
        elif (len(fail_cases) > 0):
            result = "FAIL"
            state.add_result('FAIL')
        if (len(warning_cases) != 0):
            for node in warning_cases:
                description1 += 'Node = {}, Open file descriptors = {}, Max file descriptors = {}, Percentage = {}%\n'.format(
                    node[0], node[1], node[2], node[3])
        if (len(fail_cases) != 0):
            for node in fail_cases:
                description1 += 'Node = {}, Open file descriptors = {}, Max file descriptors = {}, Percentage = {}%\n'.format(
                    node[0], node[1], node[2], node[3])
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'open_file_descriptors.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n<b>Suggestion:</b>\nUse lsof command to find all the open file descriptors to find the reason for high consumption.\n'
        description += suggestion
    list_row = [OpenFileDescriptors, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckCircuitBreakers(state, dataframe, nodes_stats, CircuitBreakers):

    warning_cases = []
    fail_cases = []
    for node in nodes_stats['nodes']:
        for breaker in nodes_stats["nodes"][node]["breakers"]:
            limit_size_in_bytes = nodes_stats["nodes"][node]["breakers"][breaker]["limit_size_in_bytes"]
            estimated_size_in_bytes = nodes_stats["nodes"][node]["breakers"][breaker]["estimated_size_in_bytes"]
            percentage = (estimated_size_in_bytes/limit_size_in_bytes)*100
            if (percentage >= 70 and percentage <= 80):
                warning_cases.append([nodes_stats["nodes"][node]["name"], breaker, round(
                    estimated_size_in_bytes/(1024**3)), round(limit_size_in_bytes/(1024**3)), percentage])
            elif (percentage > 80):
                fail_cases.append([nodes_stats["nodes"][node]["name"], breaker, round(
                    estimated_size_in_bytes/(1024**3)), round(limit_size_in_bytes/(1024**3)), percentage])
    if (len(warning_cases) == 0 and len(fail_cases) == 0):
        result = 'PASS'
        state.add_result('PASS')
        description = 'Circuit breakers limit is maintained.'
    else:
        description = '<b>Issue:</b>\nCircuit breakers limit of some nodes exceeded the threshold. Node may crash due to an OutOfMemoryError.\n'
        description1 = " "
        if (len(warning_cases) > 0 and len(fail_cases) == 0):
            result = "WARNING"
            state.add_result('WARNING')
        elif (len(fail_cases) > 0):
            result = "FAIL"
            state.add_result('FAIL')
        if (len(warning_cases) != 0):
            for node in warning_cases:
                description1 += 'Node = {}, Breaker = {}, Estimated memory used = {}GB, Total memory = {}GB, Percentage = {}%\n'.format(
                    node[0], node[1], node[2], node[3], int(node[4]))
        if (len(fail_cases) != 0):
            for node in fail_cases:
                description1 += 'Node = {}, Breaker = {}, Estimated memory used = {}GB, Total memory = {}GB, Percentage = {}%\n'.format(
                    node[0], node[1], node[2], node[3], int(node[4]))
        if (description1.count('\n') > 10):
            description_link = CreateLink(description1, 'circuit_breakers.html')
            description += 'Please find all them in the link given below.\n'+description_link+"<br>"
        else:
            description += '\n<b>Nodes:</b>\n'+description1
        suggestion = '\n <b>Suggestion:</b>\nNeeds increasing of heap size or scale out nodes.\n'
        description += suggestion
    list_row = [CircuitBreakers, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
