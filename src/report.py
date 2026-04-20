import os
import pandas as pd
import jinja2

REPORT_DIR = ""


def set_report_dir(dir_name):
    global REPORT_DIR
    REPORT_DIR = dir_name


def CreateLink(description, file_name):
    global REPORT_DIR
    whole_list = []
    header_list = []
    for row in description.split("\n"):
        if row != "":
            row_list = []
            if "," in row:
                if len(row.split(",")) > 1:
                    for value in row.split(","):
                        if "=" in value:
                            if len(header_list) < len(row.split(",")):
                                header_list.append(value.split("=")[0])
                            row_list.append(value.split("=")[1])
            elif "=" in row:
                if len(header_list) < len(row.split("=")) - 1:
                    header_list.append(row.split("=")[0])
                row_list.append(row.split("=")[1])
            while "" in row_list:
                row_list.remove("")
            whole_list.append(row_list)

    if not header_list:
        return ""

    dataframe = pd.DataFrame(whole_list, columns=header_list)
    pd.set_option("max_colwidth", 50)

    styles = [
        dict(selector='', props=[("text-align", "center"), ('background-color', 'white'),
             ('border-color', 'black'), ('border-spacing', '2px'), ('border', '1.5px solid')]),
        dict(selector='th', props=[('font-size', '12px'), ('border-style', 'solid'), ('border', '2px solid black'), ('border-width', '0.25px'), ('height',
             "25px"), ('background-color', '#0066CC'), ('color', 'white'), ('text-align', 'center'), ("font-weight", "normal"), ('vertical-align', 'left')]),
        dict(selector="tbody td", props=[("border", "1px solid grey"), ('font-size', '12px'), ('border-width', '0.25px')])
    ]

    dfnew = dataframe.style.set_table_styles(styles).hide(axis="index")

    html = f'''<html>
                <head>
                    <style>
                    table{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            width:100%;
                         }}
                     th{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: center;
                            background-color : #0066CC;
                            color:white;
                            font-size:12px;
                         }}
                     td{{
                            border : 1px solid #000000;
                            border-collapse: collapse;
                            text-align: center;
                            background-color : white;
                            font-size:12px;
                         }}
                     back_button{{
                            text-align: center;
                            background-color : #0066CC;
                            color:white;
                            font-size:12px;
                        }}
                    </style>
                </head>
                <body>
                    <form>
                        <input type="button" id = "back_button" value="Back" onclick="history.go(-1)">
                    </form>
                    <br></br>{dfnew.to_html()}
                </body>
            </html>'''

    path = os.getcwd()
    file_path = os.path.join(path, REPORT_DIR)
    os.makedirs(file_path, exist_ok=True)
    with open(os.path.join(file_path, file_name), 'w') as fp:
        fp.write(html)

    full_path = os.path.join(file_path, file_name)
    description_link = f'<a href="{full_path}">{file_name.split(".")[0]}</a>'
    return description_link
