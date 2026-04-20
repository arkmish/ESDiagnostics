HTML_TEMPLATE = '''
<html>
    <head>
        <style>
            body {{
                    background-color:white;
                }}
            h1  {{
                    color: #0066CC;
                    text-align:center;
                    font-family: Arial;
                    border-style: dotted solid double dashed;
                    border-width: 2px;
                    border-color: green;
                    border-bottom: 3px solid blue;
                    padding-top: 9px;
                    padding-right: 5px;
                    padding-bottom: 9px;
                    padding-left: 5px;
                    font-size: 23px;
                }}
            sub{{
                    font-size: 10px;
                    color: black;
                }}
            h2  {{
                    color: #0066CC;
                    text-align:center;
                    font-family: Arial;
                    font-size: 14px;
                }}
            p   {{
                    font-size:12px;
                    color:black;
                    margin-left:10px;
                }}   
            button {{
                    background-color: #0066CC;
                    border: black;
                    float: left;
                    color: white;
                    width: 50%;
                    padding: 9px 9px;
                    text-align: left;
                    text-decoration: none;
                    display: inline-block;
                    font-size: 14px;
                    margin: 0px 0px;
                    transition-duration: 0.4s;
                    cursor: pointer;
                    box-shadow: 10px 10px 20px grey;
                    border-radius: 12px;
                    font-weight: bold;
                }}
            button:hover {{
                    background-color: #1a8cff;
                }}
            button:active {{
                    background-color: #1a8cff;
                    box-shadow: 0 5px #666;
                    transform: translateY(4px);
                }}
            div1,div2,div3,div4,div5,div6{{
                    width: fit-content;
                    display: inline-block;
                    height: 100px;
                    border: 1px solid blue;
                    
                }}
            Diagnostics{{
                    width: 50%;
                    height: 100%;
                }}
            SlowLogs{{
                    width: 50%;
                    height: 100%;
                }}
            .tablink {{
                    background-color: #1b8dff;
                    text-align: center;
                    color: white;
                    float: left;
                    border: none;
                    outline: none;
                    cursor: pointer;
                    padding: 9.5px 9.5px;
                    font-size: 16px;
                    width: 12.5%;
                    margin-right:10px;
                    border-radius:2px;
                    font-family: Times New Roman;
                    box-shadow: 5px 5px 5px grey;
                    font-weight: bold;
                }}
            .tablink:hover {{
                    background-color: #1a8cff;
                }}
            .tabcontent{{
                    color: white;
                    display: none;
                    padding: 0px 0px;
                    height: 100%;
                    width:100%;  
                }}
            cluster {{
                    text-align:center;
                }}
            #container {{ 
        width: 50%; height: 50%; margin: 0; padding: 0; 
      }} 
        </style>
        <script src="https://cdn.anychart.com/releases/8.10.0/js/anychart-core.min.js"></script>
    <script src="https://cdn.anychart.com/releases/8.10.0/js/anychart-pie.min.js"></script>
    </head>
    <body>
        <h1>Elastic Search Diagnostics Report<br> <sub>cluster name : {0}</sub><br><sub>date & time : {1}</sub></h1>
        <button class="tablink" onclick="openPage('Diagnostics')" id="defaultOpen">Diagnostics</button>
        <button class="tablink" onclick="openPage('SlowLogs')" >Slow Logs</button>  
        <div id="SlowLogs" class="tabcontent">
            <br>
            <h2><u>Slow Logs</u></h2><br></br>{8}<br></br>
        </div>
        <div id="Diagnostics" class="tabcontent">
            <br></br>
            <h2><u> Elastic Search Diagnostics </u></h2><br></br>
            <article>
                <button class="button" onclick="hide1()">Summary</button> 
                <br></br>
                <script>
                    function hide1() {{
                      var x = document.getElementById("div1");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>
                <div class="hide1" id="div1" style="display:block;" >  <br></br>   
                    <div id="container"></div>
                    <script>
                    var palette = anychart.palettes.distinctColors();
                    palette.items([
                      {{ color: '#1dd05d' }},
                      {{ color: '#f60000' }},
                      {{ color: '#ffa000' }},
                      {{ color: '#156ef2' }}
                    ]);                   
                    var data = anychart.data.set({9});
                    var chart = anychart.pie(data)
                    chart.innerRadius('55%');
                    chart.title('Checks')
                    chart.container('container');
                    chart.palette(palette);
                    chart.draw();
                    </script>
                    <p><strong>Cluster Checks:<br><br></strong> {2} </br></p>
                </div>
                
            </article>
            &nbsp;&nbsp;&nbsp;
            <article>       
                <article>
                    <button onclick="hide2()">Cluster configuration</button> 
                    <br></br>
                    <script>
                        function hide2() {{
                          var x = document.getElementById("div2");
                          if (x.style.display === "none") {{
                            x.style.display = "block";
                          }} else {{
                            x.style.display = "none";
                          }}
                        }}
                    </script>
                    <div class="hide2" id="div2" style="display:none;" ><br><br> {3} </br></div>
                </article>
                &nbsp;&nbsp;
                <article>
                    <button onclick="hide3()">Elastic search overall stats usage</button> 
                    <br></br>
                    <script>
                        function hide3() {{
                          var x = document.getElementById("div3");
                          if (x.style.display === "none") {{
                            x.style.display = "block";
                          }} else {{
                            x.style.display = "none";
                          }}
                        }}
                    </script>
                    <div class="hide3" id="div3" style="display:none;" > <br> </br> {4} </div>
                </article>    
            </article>
            <br> <hr> <br>
        <article>    
            &nbsp;
            <article>
                <button onclick="hide4()">Operating system checks</button> 
                <br>
                <script>
                    function hide4() {{
                      var x = document.getElementById("div4");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>
                <div class="hide4" id="div4" style="display:none;" > <br> <br> {5} </br></div>
            </article>
            &nbsp;&nbsp;&nbsp;
            <br></br>
            <article> 
                <button onclick="hide5()">Cluster configuration checks</button>   
                <br>
                <script>
                    function hide5() {{
                      var x = document.getElementById("div5");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script> 
                <div class="hide5" id="div5" style="display:none;"> <br> <br> {6} </br></div>
            </article>
            &nbsp;&nbsp;&nbsp; 
            <br></br>
            <article> 
                <button onclick="hide6()">Elastic search stats usage checks</button>
                <br>
                <script>
                    function hide6() {{
                      var x = document.getElementById("div6");
                      if (x.style.display === "none") {{
                        x.style.display = "block";
                      }} else {{
                        x.style.display = "none";
                      }}
                    }}
                </script>  
                <div class="hide6" id="div6" style="display:none;"> <br> <br> {7} </br> </div>
            </article>
            <br style = “line-height:50; > </br>
            <hr>
            <br style = “line-height:1000; > </br>
        </article>
        </div>
        
        <script>
            function openPage(pageName) {{
              var i, tabcontent, tablinks;
              tabcontent = document.getElementsByClassName("tabcontent");
              for (i = 0; i < tabcontent.length; i++) {{
                tabcontent[i].style.display = "none";
              }}
              document.getElementById(pageName).style.display = "block";
            }}
            document.getElementById("defaultOpen").click();
        </script>   
    </body>
</html>
'''
