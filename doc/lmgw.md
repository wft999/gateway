ϵͳ�ܹ�
--------

* ��redis���Ϊ��������Ľڵ㣬�����ϱ���ڵ���redis api�����Ľڵ㽻������Ҫ��������Ϊ�������ݺ�֪ͨ��Ϣ�ķ����붩�ģ�
* �ϱ���ڵ��������ݴ洢��redis���ݿ��У��ڵ�����ʱ�����Ƚ��������Ľڵ�����ӣ�Ȼ���ȡ���ã���ɳ�ʼ����
* �ϱ���ڵ��ͨ�˿�127.0.0.1��6379����unix socket: /run/redis.sock�������Ľڵ㣻
* ����ڵ����redis���������Ľڵ㷢���ɼ����ݣ����Ľڵ�������ⶨ�Ƶ�lua������������Ԥ����Ȼ��ת�������Ľڵ㣻
* ����ڵ����redis������ѯ�������ݣ���������������������ϵͳ�Զ�ά�����Զ��ϵ�������

�ϱ���ڵ���������
------------------

* �ڵ��������ݴ洢��redis��ϣ���У��ο�����node.json�ļ���keyΪ��ϣ������ƣ�������"node:"��ͷ�������ǽڵ����ƣ�
* �ֶ���"sub:device1"��ʾ�˽ڵ㶩��device1�Ĳ������ݣ��ֶ�ֵΪ����������id����ʼֵΪ"0-0"������ֵ��ϵͳ�Զ�ά�����������������ĺͶϵ���������ֹ�����ڵ�ά������ֶΣ�
* �ֶ�"message:all"�������ã�ϵͳ�Զ����ɣ���ʾ�ڵ㶩��ϵͳ��֪ͨ��Ϣ���ֶ�ֵΪ���������ģ���ֹ�����ڵ�ά������ֶΣ�
* �ֶ�"message:self"�������ã�ϵͳ�Զ����ɣ���ʾ�ڵ㶩�������ڵ㵥�������Լ�����Ϣ������ʵ�ֽڵ�������ֹͣ����д�ȵȹ��ܣ��ֶ�ֵΪ���������ģ���ֹ�����ڵ�ά������ֶΣ�
* �ϱ���ڵ�ɸ����Լ���Ҫ������������ֶ�������node.json�ļ�����ƣ�

�ɼ���λ��������
------------------

* �趨�ֶα�־�������ʽ��lmgw.field_flags device sub_command flag field1 field2...
sub_commandΪ"set"����"reset"��flagĿǰ��֧��"accumulate"�ۼ�����־;

* �趨�ֶ�lua�����������ʽ��lmgw.field_function  device field function arg1 arg2 ...

* �趨��ʽ�ۺ����㣬�����ʽ��lmgw.field_create_rule device field AGGREGATION op period
op֧��max��min��sum��avg��twa��std.p��std.s��var.p��var.s��count��first��last��range
periodΪ����

* ɾ����ʽ�ۺ����㣬�����ʽ��lmgw.field_delete_rule device field AGGREGATION op period
op֧��max��min��sum��avg��twa��std.p��std.s��var.p��var.s��count��first��last��range
periodΪ����

lua��������
------------------

* lua����������ο�����lmgw.lua�ļ���Ŀǰֻ������һ�������ú���test_add��������������ԭ����ֵ��ӵõ�ת�����ֵ���������㺯����������ӣ�������ת����������ƣ�


�������ݷ����붩��
------------------

* ����ڵ��������"LMGW.PUB_SAMPLE"�������������ݣ������ʽ��"LMGW.PUB_SAMPLE device_name field1 value1 field2 value2 ... fieldn valuen"������ɹ������������ʱ���ID��
* ����ڵ��������"LMGW.SUB_SAMPLE"�����Ĳ������ݣ������ʽ��"LMGW.SUB_SAMPLE self_name"��self_name�����Լ��Ľڵ����ƣ�����node1,���û�����ݣ���ǰ����ǳ�ʱ1�뷵�أ����ؽ�������ݸ�ʽ��ο�����ʾ�����ٶ��Լ��Ľڵ�������node1,�ڼ�����Ϊ"node:node1"�Ĺ�ϣ���д������������ֶΣ�

    {
        "sub:device1": "0-0",
        "sub:device2": "0-0",
    }
    
����ܷ����������ݣ�

    1)1) "device1"							/*����������Դ*/
    	2) 1) 1) 1526999626221-0  			/*��������ʱ���*/
        		2) 1) "Tempature"					/*�����¶�*/
        			 2) "56.2"
        			 3) "pressure"					/*����ѹ��*/
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
    
    
֪ͨ��Ϣ�ķ����붩��
------------------

* ����ڵ��������"LMGW.PUB_MESSAGE"��������Ϣ�������ʽ��"LMGW.PUB_MESSAGE target_name field1 value1 field2 value2 ... fieldn valuen"��target_name����Ϊĳһ���ڵ����ƣ�����node1������Ϊ"all"����ʾ�������нڵ㣬����ɹ������������ʱ���ID��
* ����ڵ��������"LMGW.SUB_MESSAGE"�����Ĳ������ݣ������ʽ��"LMGW.SUB_MESSAGE self_name"��self_name�����Լ��Ľڵ����ƣ�����node1,���û�����ݣ���ǰ����ǳ�ʱ1�뷵�أ����ؽ�������ݸ�ʽ��ο�����ʾ�����ٶ��Լ��Ľڵ�������node1��


    1)1) "message:all"							/*���нڵ�����յ�֪ͨ��Ϣ*/
    	2) 1) 1) 1526999626221-0  			/*��������ʱ���*/
        		2) 1) "from"					/*��Ϣ������*/
        			 2) "manager"
        			 3) "content"					/*��Ϣ����*/
        			 4) "this is message content"        			 
    2)1) "message:node1"						/*ָ�������Լ�����Ϣ*/
    	2) 1) 1) 1526985691746-0  
        		2) 1) "from"
        			 2) "node2"
        			 3) "content"
        			 4) "this is message content"  
    

��ͨ��ѯ
------------------

* ����ڵ��������"LMGW.QUERY"����ѯ��ʷ�������ݣ������ʽ��"LMGW.QUERY device_name field_name start_time end_time [order]"��start_time��ʽΪ2018-06-01 08:00:00.000��end_time������start_timeͬ����ʽ������Ϊnow������ǰʱ�����orderΪ��ѡ�����趨ΪASC��DESC��Ĭ��ΪASC������˳�������
    
�����������ݣ�

    1)1) (integer) 1688605460						/*ʱ���*/
    	2) "1130"        			 					/*�ֶ�ֵ*/
    2)1) "device2"
    	2) "1150"    
    3)1) (integer) 1688605460						
    	2) "1120"        			 					
    4)1) "device2"
    	2) "1170"   

�ۺϲ�ѯ
------------------

* ����ڵ��������"LMGW.QUERY_AGG"���ۺ�������ʷ�������ݣ������ʽ��"LMGW.QUERY_AGG device_name field_names start_time end_time agg_names duration"��field_namesΪ���ŷָ����ֶ�����start_time��ʽΪ2018-06-01 08:00:00.000��end_time������start_timeͬ����ʽ������Ϊnow������ǰʱ�����agg_namesΪ���ŷָ��ľۺ��������ƣ�Ŀǰ֧��MIN��MAX��SUM��AVG��TWA��STD.P��STD.S��VAR.P��VAR.S��COUNT��FIRST��LAST��RANGE��duration�Ǻ��뵥λ�ľۺ��������ڣ�ʾ����ʽ��
lmgw.query_agg device2 field1,field2 "2023-08-13 08:31:00.000" "2023-08-13 09:41:00.000" min,max,first,last,count 60000


�ڵ����
------------------
* �ڵ������ʽ��lmgw.node node_name op
opΪstart����stop
    

���Ի���
------------------

* �Ʒ���������������redis:sudo /usr/local/bin/redis-server /etc/redis/lmgw.conf;
* �����󼴿�ͨ���˿�127.0.0.1��6379����unix socket: /run/redis.sock���ӣ�