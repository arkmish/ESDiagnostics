import statistics
from src.report import CreateLink

acceptable_standard_deviation = 5


def CheckClusterConcurrentRebalance(state, dataframe, cluster_settings, ClusterConcurrentRebalance):

    # TODO. Some times cluster_concurrent_rebalance may be present under defaults.cluster.routing.allocation or under transient.cluster.routing.allocation or under persistent.cluster.routing.allocation. Below logic needs to be changed to check both and pick the value where ever it is present.
    # Fixed TODO

    try:
        cluster_concurrent_rebalance = cluster_settings["transient"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass
    try:
        cluster_concurrent_rebalance = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass
    try:
        cluster_concurrent_rebalance = cluster_settings["persistent"]["cluster"]["routing"]["allocation"]["cluster_concurrent_rebalance"]
    except KeyError:
        pass

    if (cluster_concurrent_rebalance == '2'):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.routing.allocation.cluster_concurrent_rebalance = {}'.format(cluster_concurrent_rebalance)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.routing.allocation.cluster_concurrent_rebalance = {}\nCluster may not be able to rebalance shards as the concurrent rebalance setting is not maintained.\n\n<b>Suggestion:</b>\nTo change the concurrent rebalance settings, use \nPUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.cluster_concurrent_rebalance": 2
                          }}
                        }}'''.format(cluster_concurrent_rebalance)
    list_row = [ClusterConcurrentRebalance, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckClusterEnableRebalance(state, dataframe, cluster_settings, ClusterEnableRebalance):

    cluster_enable_rebalance = cluster_settings["defaults"]["cluster"]["routing"]["rebalance"]["enable"]
    if (cluster_enable_rebalance == "all"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.routing.rebalance.enable = {}'.format(cluster_enable_rebalance)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.routing.rebalance.enable = {}\nCluster rebalance is disabled.\n\n<b>Suggestion:</b>\nTo enable automatic cluster rebalancing, use 
                        PUT /_cluster/settings?flat_settings=true
                        {{
                            "defaults" : {{
                                "cluster.routing.rebalance.enable": "all",     
                            }}
                        }}'''.format(cluster_enable_rebalance)
    list_row = [ClusterEnableRebalance, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckClusterEnableAllocation(state, dataframe, cluster_settings, ClusterEnableAllocation):

    cluster_enable_allocation = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["enable"]
    if (cluster_enable_allocation == "all"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.routing.allocation.enable = {}'.format(cluster_enable_allocation)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.routing.allocation.enable = {}\nCluster allocation is disabled. \n\n<b>Suggestion:</b>\nTo enable, use \nPUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.enable": "all"
                          }}
                        }}'''.format(cluster_enable_allocation)
    list_row = [ClusterEnableAllocation, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckAdaptiveReplicaSelection(state, dataframe, cluster_settings, AdaptiveReplicaSelection):

    if "use_adaptive_replica_selection" in cluster_settings:
        if "use_adaptive_replica_selection" in cluster_settings["transient"]:
            use_adaptive_replica_selection = cluster_settings["transient"]["cluster"]["routing"]["use_adaptive_replica_selection"]
        else:
            use_adaptive_replica_selection = cluster_settings["defaults"]["cluster"]["routing"]["use_adaptive_replica_selection"]
    else:
        use_adaptive_replica_selection = "false"

    if (use_adaptive_replica_selection == "true"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.routing.use_adaptive_replica_selection = {}'.format(use_adaptive_replica_selection)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.routing.use_adaptive_replica_selection = {}\nCluster is not using adaptive replica selection.\n\n<b>Suggestion:</b>\nTo enable, use
                        PUT /_cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.use_adaptive_replica_selection": true
                          }}
                        }}'''.format(use_adaptive_replica_selection)
    list_row = [AdaptiveReplicaSelection, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckUsageOfWildcards(state, dataframe, cluster_settings, UsageOfWildcards):

    destructive_requires_name = cluster_settings["defaults"]["action"]["destructive_requires_name"]
    if (destructive_requires_name == "true"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'action.destructive_requires_name = {}'.format(destructive_requires_name)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\naction.destructive_requires_name = {}\nWildcard usage is enabled.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT /_cluster/settings
                        {{
                          "defaults": {{
                            "action.destructive_requires_name":true
                          }}
                        }}'''.format(destructive_requires_name)
    list_row = [UsageOfWildcards, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckAllowLeadingWildcard(state, dataframe, cluster_settings, AllowLeadingWildcard):

    allow_leading_wildcard = cluster_settings["defaults"]["indices"]["query"]["query_string"]["allowLeadingWildcard"]
    if (allow_leading_wildcard == "false"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'indices.query.query_string.allowLeadingWildcard = {}'.format(allow_leading_wildcard)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\nindices.query.query_string.allowLeadingWildcard = {}\nLeading wildcard is enabled. This will create huge impact on overall cluster if someone tries queries with * as leading character.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "indices.query.query_string.allowLeadingWildcard": false
                          }}
                        }}'''.format(allow_leading_wildcard)
    list_row = [AllowLeadingWildcard, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckOpenScrollContext(state, dataframe, cluster_settings, OpenScrollContext):

    if ("search" in cluster_settings["persistent"]):
        max_open_scroll_context = cluster_settings["persistent"]["search"]["max_open_scroll_context"]
        if (int(max_open_scroll_context) == 500):
            result = 'PASS'
            state.add_result('PASS')
            description = 'persistent.search.max_open_scroll_context = {}\nOpen scroll context limit is not reached yet. However, elasticsearch does not allow more than 500 open scroll contexts. '.format(
                max_open_scroll_context)
        else:
            result = 'FAIL'
            state.add_result('FAIL')
            description = '''<b>Issue:</b>\npersistent.search.max_open_scroll_context = {}\nOpen scroll context limit is reached. Elasticsearch does not allow more than 500 open scroll contexts.\n\n<b>Suggestion:</b>\nUse below api to set the limit for Scrolls.\nPUT localhost:9200/_cluster/settings 
            {{
                "persistent" : {{
                    "search.max_open_scroll_context": 500
                }}
            }}'''.format(max_open_scroll_context)
    else:
        max_open_scroll_context = cluster_settings["defaults"]["search"]["max_open_scroll_context"]
        if (int(max_open_scroll_context) == 500):
            result = 'PASS'
            state.add_result('PASS')
            description = 'defaults.search.max_open_scroll_context = {}\nOpen scroll context limit is not reached yet. However, elasticsearch does not allow more than 500 open scroll contexts. '.format(
                max_open_scroll_context)
        else:
            result = 'FAIL'
            state.add_result('FAIL')
            description = '''<b>Issue:</b>\ndefaults.search.max_open_scroll_context = {}\nOpen scroll context limit is reached. Elasticsearch does not allow more than 500 open scroll contexts.\n\n<b>Suggestion:</b>\nUse below api to set the limit for Scrolls.\nPUT localhost:9200/_cluster/settings 
            {{
                "defaults" : {{
                    "search.max_open_scroll_context": 500
                }}
            }}'''.format(max_open_scroll_context)

    list_row = [OpenScrollContext, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckNodeConcurrentRecovery(state, dataframe, cluster_settings, NodeConcurrentRecovery):

    # TODO node_concurrent_recoveries may present under transient, defaults, or persistent. Pick the right place to validate it
    # Fixed TODO
    try:
        node_concurrent_recoveries = cluster_settings["transient"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass
    try:
        node_concurrent_recoveries = cluster_settings["defaults"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass
    try:
        node_concurrent_recoveries = cluster_settings["persistent"]["cluster"]["routing"]["allocation"]["node_concurrent_recoveries"]
    except KeyError:
        pass

    if (node_concurrent_recoveries == "20"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.routing.allocation.node_concurrent_recoveries = {}'.format(node_concurrent_recoveries)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.routing.allocation.node_concurrent_recoveries = {}\nThe concurrent recoveries setting is set too low. \n\n<b>Suggestion:</b>\nTo change the concurrent recovery settings, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.routing.allocation.node_concurrent_recoveries ": 20
                          }}
                        }}'''.format(node_concurrent_recoveries)
    list_row = [NodeConcurrentRecovery, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckReadOnlyAllowDelete(state, dataframe, cluster_settings, ReadOnlyAllowDelete):

    read_only_allow_delete = cluster_settings["defaults"]["cluster"]["blocks"]["read_only_allow_delete"]
    if (read_only_allow_delete == "null" or read_only_allow_delete == "false"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'cluster.blocks.read_only_allow_delete = {}'.format(read_only_allow_delete)
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\ncluster.blocks.read_only_allow_delete = {}\nRead-only-allow-delete index(s) found. Cluster disk usage might have been hit high water mark causing this.\n\n<b>Suggestion:</b>\nIncrease the disk space and run below api to reset it.
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "cluster.blocks.read_only_allow_delete":null
                          }}
                        }}'''.format(read_only_allow_delete)
    list_row = [ReadOnlyAllowDelete, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe


def CheckPainlessRegex(state, dataframe, cluster_settings, PainlessRegex):

    painless_regex_enable = cluster_settings["defaults"]["script"]["painless"]["regex"]["enabled"]
    if (painless_regex_enable == "false"):
        result = 'PASS'
        state.add_result('PASS')
        description = 'regex is disabled in Elasticsearch painless scripts.'
    else:
        result = 'FAIL'
        state.add_result('FAIL')
        description = '''<b>Issue:</b>\nscript.painless.regex.enabled = {}.\n\n<b>Suggestion:</b>\nTo disable, use 
                        PUT _cluster/settings
                        {{
                          "defaults": {{
                            "script.painless.regex.enabled":false
                          }}
                        }}'''.format(painless_regex_enable)
    list_row = [PainlessRegex, result, description]
    dataframe.loc[len(dataframe)] = list_row
    return dataframe
