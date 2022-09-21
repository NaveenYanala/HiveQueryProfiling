import time

import requests


class AmbariParser(object):

    def __init__(self, job_start_epoch=0, limit=10,last_hive_query_id=""):
        self.TIMELINE_URI = "http://172.21.58.37:8080/api/v1/views/TEZ/versions/0.7.0.2.6.2.0-205/instances/TEZ_CLUSTER_INSTANCE/resources/atsproxy/ws/v1/timeline/HIVE_QUERY_ID"
        self.job_start_epoch = int(time.time()) if job_start_epoch == 0 else job_start_epoch
        self.limit = limit
        self.all_summary = list()
        self.last_hive_query_id=last_hive_query_id

    @staticmethod
    def connect():
        pass

    def get_start_time(self):
        print(self.job_start_epoch)

    def get_dag_summary(self):
        if self.last_hive_query_id=="":
            dag_summary = requests.get(self.TIMELINE_URI, params={'limit': self.limit, '_': self.job_start_epoch,
                                                                  },
                                       cookies={'AMBARISESSIONID': '1t5pgls8bet83di5luyvy8p8n'})
            # print(f'if dag detaisl {dag_summary.url}')
        else:
            dag_summary = requests.get(self.TIMELINE_URI, params={'limit': self.limit, '_': self.job_start_epoch,
                                                                  'fromId': self.last_hive_query_id},
                                       cookies={'AMBARISESSIONID': '1t5pgls8bet83di5luyvy8p8n'})
            # print(f'dag detaisl {dag_summary.url}')
        temp=list()
        temp.append(dag_summary.json())
        # print('else_dag_summary')
        return temp

    def get_all_summary(self):
        k = self.get_dag_summary(self.limit)
        self.all_summary.append(k)
        counter = 0
        while counter < 5:
            self.job_start_epoch = self.job_start_epoch + 5 * 60 * 1000
            self.all_summary.append(self.get_dag_summary(self.limit))
            counter = counter + 1
        return self.all_summary
