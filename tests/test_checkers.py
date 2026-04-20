import pytest
import pandas as pd
from src.state import DiagnosticState
from src.checkers_os import CheckCPULoadAverage, CheckMemoryUsagePercent


@pytest.fixture
def mock_state():
    return DiagnosticState()


@pytest.fixture
def mock_empty_df():
    return pd.DataFrame(columns=['Diagnostic check', 'Status', 'Description'])


def test_check_memory_usage_percent_pass(mock_state, mock_empty_df):
    node_stats = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'os': {'mem': {'used_percent': 70}}
            }
        }
    }
    
    df = CheckMemoryUsagePercent(mock_state, mock_empty_df, node_stats, "Memory Usage Percent")
    
    assert mock_state.total_pass_checks == 1
    assert df.iloc[0]['Status'] == 'PASS'
    assert 'Memory consumption of all nodes is < 85%' in df.iloc[0]['Description']


def test_check_memory_usage_percent_fail(mock_state, mock_empty_df):
    node_stats = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'os': {'mem': {'used_percent': 95}}
            }
        }
    }
    
    df = CheckMemoryUsagePercent(mock_state, mock_empty_df, node_stats, "Memory Usage Percent")
    
    assert mock_state.total_fail_checks == 1
    assert df.iloc[0]['Status'] == 'FAIL'
    assert '> 85%' in df.iloc[0]['Description']


def test_check_cpu_load_average_pass(mock_state, mock_empty_df):
    node_stats = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'os': {'cpu': {'load_average': {'15m': 1.0}}}
            }
        }
    }
    nodes = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'thread_pool': {
                    'pool1': {'core': 4}
                }
            }
        }
    }
    
    # Core count = 4. load_average = 1.0. 1.0 / 4 is 0.25 (pass)
    df = CheckCPULoadAverage(mock_state, mock_empty_df, node_stats, nodes, "CPU Load Average")
    
    assert mock_state.total_pass_checks == 1
    assert df.iloc[0]['Status'] == 'PASS'


def test_check_cpu_load_average_fail(mock_state, mock_empty_df):
    node_stats = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'os': {'cpu': {'load_average': {'15m': 4.0}}}
            }
        }
    }
    nodes = {
        'nodes': {
            'node1': {
                'name': 'test-node-1',
                'thread_pool': {
                    'pool1': {'core': 4}
                }
            }
        }
    }
    
    # Core count = 4. load_average = 4.0. 4.0 > 0.8 * 4 (3.2), so fail
    df = CheckCPULoadAverage(mock_state, mock_empty_df, node_stats, nodes, "CPU Load Average")
    
    assert mock_state.total_fail_checks == 1
    assert df.iloc[0]['Status'] == 'FAIL'
