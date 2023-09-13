系统架构
--------

* 以redis框架为基础搭建中心节点，所有南北向节点以redis api与中心节点交互，主要交互内容为采样数据和通知消息的发布与订阅；
* 南北向节点配置数据存储在redis数据库中，节点启动时，首先建立与中心节点的连接，然后读取配置，完成初始化；
* 南北向节点可通端口127.0.0.1：6379或者unix socket: /run/redis.sock连接中心节点；
* 南向节点调用redis命令向中心节点发布采集数据，中心节点调用任意定制的lua函数进行数据预处理，然后转发各订阅节点；
* 北向节点调用redis命令轮询采样数据，采样数据推送上下文由系统自动维护，自动断点续传；

南北向节点配置数据
------------------

* 节点配置数据存储在redis哈希表中，参看附件node.json文件，key为哈希表键名称，必须以"node:"开头，后面是节点名称；
* 字段名"sub:device1"表示此节点订阅device1的采样数据，字段值为采样数据流id，初始值为"0-0"，后续值由系统自动维护，用于推送上下文和断点续传，禁止其他节点维护这个字段；
* 字段"message:all"无需配置，系统自动生成，表示节点订阅系统的通知消息，字段值为推送上下文，禁止其他节点维护这个字段；
* 字段"message:self"无需配置，系统自动生成，表示节点订阅其他节点单独发给自己的消息，比如实现节点启动、停止、反写等等功能，字段值为推送上下文，禁止其他节点维护这个字段；
* 南北向节点可根据自己需要添加任意配置字段以完善node.json文件的设计；

采集点位配置数据
------------------

* 设定字段标志，命令格式：lmgw.field_flags device sub_command flag field1 field2...
sub_command为"set"或者"reset"，flag目前仅支持"accumulate"累计量标志;

* 设定字段lua函数，命令格式：lmgw.field_function  device field function arg1 arg2 ...

* 设定流式聚合运算，命令格式：lmgw.field_create_rule device field AGGREGATION op period
op支持max、min、sum、avg、twa、std.p、std.s、var.p、var.s、count、first、last、range
period为毫秒

* 删除流式聚合运算，命令格式：lmgw.field_delete_rule device field AGGREGATION op period
op支持max、min、sum、avg、twa、std.p、std.s、var.p、var.s、count、first、last、range
period为毫秒

lua函数定制
------------------

* lua函数定制请参看附件lmgw.lua文件，目前只定义了一个测试用函数test_add，带三个参数与原采样值相加得到转换后的值，其他计算函数可类似添加，逐步完善转换函数的设计；


采样数据发布与订阅
------------------

* 南向节点调用命令"LMGW.PUB_SAMPLE"来发布采样数据，命令格式："LMGW.PUB_SAMPLE device_name field1 value1 field2 value2 ... fieldn valuen"，如果成功发布，命令返回时间戳ID；
* 北向节点调用命令"LMGW.SUB_SAMPLE"来订阅采样数据，命令格式："LMGW.SUB_SAMPLE self_name"，self_name就是自己的节点名称，比如node1,如果没有数据，当前设计是超时1秒返回；返回结果的数据格式请参看如下示例，假定自己的节点名称是node1,在键名称为"node:node1"的哈希表中存在如下两个字段：

    {
        "sub:device1": "0-0",
        "sub:device2": "0-0",
    }
    
则可能返回如下内容：

    1)1) "device1"							/*采样数据来源*/
    	2) 1) 1) 1526999626221-0  			/*采样数据时间戳*/
        		2) 1) "Tempature"					/*采样温度*/
        			 2) "56.2"
        			 3) "pressure"					/*采样压力*/
        			 4) "2.3"        			 
    2)1) "device2"
    	2) 1) 1) 1526985691746-0  
        		2) 1) "Tempature"
        			 2) "56.2"
        			 3) "pressure"
        			 4) "2.3"  
    		 2) 1) 1526985712947-0  
        		2) 1) "Tempature"
        			 2) "56.2"
        			 3) "pressure"
        			 4) "2.3"      
    
    
通知消息的发布与订阅
------------------

* 南向节点调用命令"LMGW.PUB_MESSAGE"来发布消息，命令格式："LMGW.PUB_MESSAGE target_name field1 value1 field2 value2 ... fieldn valuen"，target_name或者为某一个节点名称，比如node1，或者为"all"，表示发给所有节点，如果成功发布，命令返回时间戳ID；
* 北向节点调用命令"LMGW.SUB_MESSAGE"来订阅采样数据，命令格式："LMGW.SUB_MESSAGE self_name"，self_name就是自己的节点名称，比如node1,如果没有数据，当前设计是超时1秒返回；返回结果的数据格式请参看如下示例，假定自己的节点名称是node1：


    1)1) "message:all"							/*所有节点需接收的通知消息*/
    	2) 1) 1) 1526999626221-0  			/*采样数据时间戳*/
        		2) 1) "from"					/*消息发布者*/
        			 2) "manager"
        			 3) "content"					/*消息内容*/
        			 4) "this is message content"        			 
    2)1) "message:node1"						/*指定发给自己的消息*/
    	2) 1) 1) 1526985691746-0  
        		2) 1) "from"
        			 2) "node2"
        			 3) "content"
        			 4) "this is message content"  
    

普通查询
------------------

* 北向节点调用命令"LMGW.QUERY"来查询历史采样数据，命令格式："LMGW.QUERY device_name field_name start_time end_time [order]"，start_time格式为2018-06-01 08:00:00.000，end_time或者与start_time同样格式，或者为now，代表当前时间戳；order为可选，可设定为ASC或DESC，默认为ASC：递增顺序输出；
    
返回如下内容：

    1)1) (integer) 1688605460						/*时间戳*/
    	2) "1130"        			 					/*字段值*/
    2)1) "device2"
    	2) "1150"    
    3)1) (integer) 1688605460						
    	2) "1120"        			 					
    4)1) "device2"
    	2) "1170"   

聚合查询
------------------

* 北向节点调用命令"LMGW.QUERY_AGG"来聚合运算历史采样数据，命令格式："LMGW.QUERY_AGG device_name field_names start_time end_time agg_names duration"，field_names为逗号分隔的字段名，start_time格式为2018-06-01 08:00:00.000，end_time或者与start_time同样格式，或者为now，代表当前时间戳；agg_names为逗号分隔的聚合运算名称，目前支持MIN、MAX、SUM、AVG、TWA、STD.P、STD.S、VAR.P、VAR.S、COUNT、FIRST、LAST、RANGE，duration是毫秒单位的聚合运算周期；示例格式：
lmgw.query_agg device2 field1,field2 "2023-08-13 08:31:00.000" "2023-08-13 09:41:00.000" min,max,first,last,count 60000


节点控制
------------------
* 节点命令格式：lmgw.node node_name op
op为start或者stop
    

测试环境
------------------

* 云服务器命令行启动redis:sudo /usr/local/bin/redis-server /etc/redis/lmgw.conf;
* 启动后即可通过端口127.0.0.1：6379或者unix socket: /run/redis.sock连接；