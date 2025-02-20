Team coding project - Building a DAG in the Cloud

- Connect to EC2 with :  ssh -i path/to/key.pem ec2-user@IPv4address
- Create working folder : dag-project
- create env inside working folder 
- install libraries inside working folder ( venv / pip ...) (apache-airflow, apache-airflow- ... , psycopg2)
0
-  set up dag folder in airflow : $export AIRFLOW_HOME=~/airflow     $nano ~/airflow/airflow.cfg 
- install git ( outside env) : sudo yum install git -y
- install postgres : sudo yum install postgres -y (it was slightly more complicated for postgres but its done (check postgres website for more info))
[ec2-user@ip-172-31-16-207 ~]$cat /etc/os-release
[ec2-user@ip-172-31-16-207 ~]$ sudo dnf install postgresql15.x86_64 postgresql15-server -y

- airflow connections for transactional database and analytical database created 
- visit airflow UI by EC2 public IP address : http://18.130.226.177:8080


Good to know : 
 - if when using airflow webserver -p 8080  or airflow scheduler we get message " error , already in use ... " or something like that , we can use "$sudo lsof -i :8080 "  to get the servers that are being used and kill them by $sudo kill -9 {PID} >> pid example : 12543

 - venv name : airflow_venv

 - dag folder : /home/ec2-user/dag-project/do-you-like-dags
      
