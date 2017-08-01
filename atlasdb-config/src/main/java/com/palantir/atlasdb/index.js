<!DOCTYPE html>
<!--~
  ~ Copyright 2017 Palantir Technologies, Inc. All rights reserved.
  ~
  ~ Licensed under the BSD-3 License (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~ http://opensource.org/licenses/BSD-3-Clause
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>

    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
    <script>
        $(document).ready(function(){
            $("#getTables").click(function(){
                $.post("/console2/tables",
                    {
                      query: 1
                    },
                    function(data,status){
                        $("#results_container").html(data);
                        console.log(data);
                    }
                );
            });

            $("#getMetadata").click(function(){
                 $.post("/console2/metadata",
                     {
                       table: document.getElementById('tableName').value
                     },
                     function(data,status){
                         $("#results_container").html(data);
                         console.log(data);
                     }
                 );
             });
        });
    </script>
</head>
<body>
    <button id="getTables">Get All Tables</button> <br>
    <input type="text" id="tableName" value="Enter table name">
    <button id="getMetadata">Get Table Metadata</button> <br>
    <div id="results_container"></div>
</body>
</html>