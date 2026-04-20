import dask.utils


def color_string(result):
    """Return CSS color property based on status string."""
    if result == 'PASS':
        color = 'green'
    elif result == 'WARNING':
        color = 'orange'
    elif result == 'FAIL':
        color = 'red'
    else:
        color = 'black'
    return f'color: {color}'


def red_color(data):
    """Return CSS style color red if limits exceeded."""
    if isinstance(data, str) and "/" in data:
        store_list = data.split("/")
        if len(store_list) <= 2:
            used = dask.utils.parse_bytes(store_list[0])
            total = dask.utils.parse_bytes(store_list[1])
            if used > 0.8 * total:
                return 'color: red'
        elif len(store_list) > 2:
            total = int(store_list[0].split(":")[1])
            active = int(store_list[1].split(":")[1])
            if active > 0.8 * total:
                return 'color: red'
    return 'color: black'


def highlight(df):
    """Highlight specific rows with blue background."""
    config = df.Configuration
    if config in ['Basic Configuration', 'OS and JVM', 'Threadpool Usage']:
        return ['background-color : #0066CC'] * df.size
    return ['background-color : white'] * df.size
