import os
import shutil
import argparse
import datetime
import pandas as pd
import warnings

from src.data_loader import ESDataLoader
from src.state import DiagnosticState
from src.checkers_os import (CheckCPULoadAverage, CheckMemoryUsagePercent, CheckDiskUsageLimits,
                             CheckDiskUsageDistribution, CheckSwap)
from src.checkers_es import (CheckStatus, CheckIsMasterRequired, CheckRelo, CheckActiveShardsPercentage,
                             CheckShardsDistribution, CheckTotalShardsPerNode, CheckShardsPerIndex,
                             CheckUnassignedShards, CheckClusterPendingTasks, CheckTasks,
                             CheckHeapSizeDataNodes, CheckHeapSizeMasterNodes, CheckTotalMemoryDataNodes,
                             CheckThreadpoolUsage, CheckOpenFileDescriptors, CheckCircuitBreakers,
                             CheckSegmentFragmentation, CheckEmptyIndices)
from src.checkers_config import (CheckClusterConcurrentRebalance, CheckClusterEnableRebalance,
                                 CheckClusterEnableAllocation, CheckAdaptiveReplicaSelection,
                                 CheckUsageOfWildcards, CheckAllowLeadingWildcard, CheckOpenScrollContext,
                                 CheckNodeConcurrentRecovery, CheckReadOnlyAllowDelete, CheckPainlessRegex)
from src.checkers_stats import ClusterConfiguration, OverallStats, SlowLog
from src.report import set_report_dir
from src.template import HTML_TEMPLATE
from src.utils import color_string

warnings.filterwarnings("ignore", category=FutureWarning)


def GetClusterNameAndDateTime(cat_health):
    for cluster in cat_health:
        epoch_time = float(cluster["epoch"])
        cluster_name = cluster["cluster"]
        date_time = str(datetime.datetime.fromtimestamp(epoch_time))
    date = str(date_time.replace(":", "").replace("-", "")).split(" ")
    folder_name = 'PSR_' + cluster_name + "_" + date[0] + "T" + date[1]

    path = os.getcwd()
    dir_path = os.path.join(path, folder_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    else:
        shutil.rmtree(dir_path, ignore_errors=True)
        os.makedirs(dir_path)
    return cluster_name, date_time, folder_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-StatsZipFile', action="store", dest='StatsZipFile', type=str, help='Add Stats Zip File')
    parser.add_argument('-secure', action="store", dest='secure', type=str, help='Add Secure/Unsecure')
    parser.add_argument('-CanAccessCluster', action="store", dest='CanAccessCluster', type=str, help='CanAccessCluster/Not')
    parser.add_argument('-URL', action="store", dest='es_url', type=str, help='Add es_url')
    parser.add_argument('-Username', action="store", dest='username', type=str, help='Add username')
    parser.add_argument('-password', action="store", dest='password', type=str, help='Add password')
    args = parser.parse_args()

    # Credential processing
    username = args.username or os.environ.get("ES_USERNAME")
    password = args.password or os.environ.get("ES_PASSWORD")
    secure_mode = args.secure == 'True'
    can_access = args.CanAccessCluster == 'True'

    loader = ESDataLoader(
        es_url=args.es_url,
        username=username,
        password=password,
        secure=secure_mode
    )

    if can_access:
        data = loader.load_from_cluster()
        print("Cluster stats successfully loaded!")
    elif args.CanAccessCluster == 'False' or args.StatsZipFile:
        if not args.StatsZipFile:
            print("Please provide -StatsZipFile argument.")
            exit(1)
        data = loader.load_from_zip(args.StatsZipFile)
    else:
        print("Usage error. Specify CanAccessCluster True or False with a StatsZipFile.")
        exit(1)

    # State initialization
    state = DiagnosticState()

    # Extract required cluster artifacts
    cat_health = data['cat_health']
    nodes_stats = data['nodes_stats']
    cat_nodes = data['cat_nodes']
    cat_shards = data['cat_shards']
    cluster_health = data['cluster_health']
    cat_pending_tasks = data['cat_pending_tasks']
    cluster_pending_tasks = data['cluster_pending_tasks']
    cat_allocation = data['cat_allocation']
    tasks = data['tasks']
    cat_indices = data['cat_indices']
    nodes = data['nodes']
    cluster_settings = data['cluster_settings']
    slow_logs = data.get('slow_logs', [])

    cluster_name, date_time, folder_name = GetClusterNameAndDateTime(cat_health)
    set_report_dir(folder_name)

    cluster_config_df = ClusterConfiguration(nodes, cat_nodes)
    df_overall, disk_data = OverallStats(nodes_stats, cat_allocation)

    if slow_logs:
        slow_log_dataframe = SlowLog(slow_logs)
        slow_log_html = slow_log_dataframe.to_html()
    else:
        slow_log_html = 'No slowlogs'

    # Check setup Dataframes
    os_df = pd.DataFrame(columns=['Diagnostic check', 'Status', 'Description'])
    es_df = pd.DataFrame(columns=['Diagnostic check', 'Status', 'Description'])
    config_df = pd.DataFrame(columns=['Diagnostic check', 'Status', 'Description'])

    # OS Checks
    os_df = CheckCPULoadAverage(state, os_df, nodes_stats, nodes, 'CPU Load Average')
    os_df = CheckMemoryUsagePercent(state, os_df, nodes_stats, 'Memory Usage')
    os_df = CheckDiskUsageLimits(state, os_df, cat_allocation, 'Disk Usage')
    os_df = CheckDiskUsageDistribution(state, os_df, cat_allocation, 'Disk Usage Distribution')
    os_df = CheckSwap(state, os_df, nodes_stats, 'Swap')

    # ES Stats Checks
    es_df = CheckStatus(state, es_df, cluster_health, 'Cluster Health')
    es_df = CheckIsMasterRequired(state, es_df, cat_nodes, 'Is Dedicated Master Required')
    es_df = CheckRelo(state, es_df, cluster_health, 'Indices Relocation Count')
    es_df = CheckActiveShardsPercentage(state, es_df, cluster_health, 'Active Shards Percentage')
    es_df = CheckShardsDistribution(state, es_df, cat_allocation, 'Shards Distribution')
    es_df, total_shards, total_nodes, total_heap = CheckTotalShardsPerNode(state, es_df, nodes_stats, cat_allocation, 'Total Shards Per Node')
    es_df = CheckShardsPerIndex(state, es_df, cat_indices, 'Shards Per Index')
    es_df = CheckUnassignedShards(state, es_df, cat_shards, 'Unassigned Shards Count')
    es_df = CheckClusterPendingTasks(state, es_df, cluster_pending_tasks, 'Cluster Pending Tasks')
    es_df = CheckTasks(state, es_df, tasks, 'High Time-Consuming Tasks')
    es_df = CheckHeapSizeDataNodes(state, es_df, nodes_stats, 'Heap Size for Data Nodes')
    es_df = CheckHeapSizeMasterNodes(state, es_df, nodes_stats, cat_nodes, 'Heap Size for Dedicated Master Nodes')
    es_df = CheckTotalMemoryDataNodes(state, es_df, nodes_stats, 'Total Memory of Data Nodes')
    es_df = CheckThreadpoolUsage(state, es_df, nodes_stats, 'Threadpool Usage')
    es_df = CheckOpenFileDescriptors(state, es_df, nodes_stats, 'Open File Descriptors')
    es_df = CheckCircuitBreakers(state, es_df, nodes_stats, 'Circuit Breakers')
    es_df = CheckSegmentFragmentation(state, es_df, cat_indices, 'Segment Fragmentation Level')
    es_df = CheckEmptyIndices(state, es_df, cat_indices, 'Empty Indices')

    # Config Checks
    config_df = CheckClusterConcurrentRebalance(state, config_df, cluster_settings, 'Number of Concurrent Shard Rebalance')
    config_df = CheckClusterEnableRebalance(state, config_df, cluster_settings, 'Allow Rebalance of all Shards')
    config_df = CheckClusterEnableAllocation(state, config_df, cluster_settings, 'Allow Allocation of all Shards')
    config_df = CheckAdaptiveReplicaSelection(state, config_df, cluster_settings, 'Use Adaptive Replica Selection')
    config_df = CheckUsageOfWildcards(state, config_df, cluster_settings, 'Allow Wildcard Usage')
    config_df = CheckAllowLeadingWildcard(state, config_df, cluster_settings, 'Allow Leading Wildcard Usage')
    config_df = CheckOpenScrollContext(state, config_df, cluster_settings, 'Open Scroll Context Limit')
    config_df = CheckNodeConcurrentRecovery(state, config_df, cluster_settings, 'Node Concurrent Recovery')
    config_df = CheckReadOnlyAllowDelete(state, config_df, cluster_settings, 'Any Read Only Indices')
    config_df = CheckPainlessRegex(state, config_df, cluster_settings, 'Using Regex in Painless Scripts')

    active_shards = cluster_health["active_shards"]
    total_space = total_nodes * 25 * total_heap
    total_checks = len(os_df) + len(config_df) + len(es_df)

    es_df['Description'] = es_df['Description'].str.replace('\n', '<br>')
    os_df['Description'] = os_df['Description'].str.replace('\n', '<br>')
    config_df['Description'] = config_df['Description'].str.replace('\n', '<br>')

    es_df['Description'] = es_df['Description'].str.wrap(100)
    os_df['Description'] = os_df['Description'].str.wrap(100)
    config_df['Description'] = config_df['Description'].str.wrap(100)

    styles = [
        dict(selector='', props=[("text-align", "center"), ('background-color', 'white'),
             ('border-color', 'black'), ('border-spacing', '2px'), ('border', '1.5px solid')]),
        dict(selector='th', props=[('font-size', '12px'), ('border-style', 'solid'), ('border', '2px solid black'), ('border-width', '0.25px'), ('height',
             "25px"), ('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'), ("font-weight", "normal"), ('vertical-align', 'center')]),
        dict(selector="tbody td", props=[("border", "1px solid grey"), ('font-size', '12px'), ('border-width', '0.25px')])
    ]

    es_df = es_df.style.map(color_string, subset=["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(
        subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide(axis="index")
    os_df = os_df.style.map(color_string, subset=["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(
        subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide(axis="index")
    config_df = config_df.style.map(color_string, subset=["Status"]).set_table_styles(styles).set_properties(subset=['Status'], **{'width': '150px'}).set_properties(
        subset=['Diagnostic check'], **{'width': '250px'}).set_properties(subset=['Description'], **{'text-align': 'left'}).hide(axis="index")

    checks = state.to_list()
    statement = f"Total number of checks : {total_checks} <br> Passed: {state.total_pass_checks}; Warning: {state.total_warning_checks}; Failed: {state.total_fail_checks}; UnknownError: {state.total_unknown_error_checks}.<br></br>"
    statement += f"<p><strong>Cluster Information: <br></strong> Total nodes : {total_nodes}<br>Total shards : {total_shards}<br> Active shards : {active_shards}<br>Total space : {total_space}GB<br>"
    for node in disk_data:
        statement += f"Node : {node[0]} disk used : {node[1]} disk allocated : {node[2]}<br>"

    statement += '</p>'
    text = HTML_TEMPLATE.format(cluster_name, str(date_time), statement, cluster_config_df.to_html(), df_overall.to_html(),
                                os_df.to_html(), config_df.to_html(), es_df.to_html(), slow_log_html, checks)

    main_file_name = 'PSRElasticSearchDiagnosticsReport.html'
    path = os.getcwd()
    file_path = os.path.join(path, folder_name)
    with open(os.path.join(file_path, main_file_name), 'w', encoding='utf-8') as fp:
        fp.write(text)

    shutil.make_archive(folder_name, 'zip', folder_name)
    print(f"Generated {main_file_name} under {folder_name} and zipped.")


if __name__ == '__main__':
    main()
