#coding:utf8
'''Author leoatchina'''
import os
import collections
import signal
import csv
import re
import datetime
import subprocess
from multiprocessing import cpu_count
from multiprocessing import Pool
# from multiprocessing.dummy import Pool as ThreadPool


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def read_csv(csv_file, delimiter=",", encoding="utf-8"):
    with open(csv_file, encoding = encoding) as csv_handle:
        csv_reader = csv.reader(csv_handle,delimiter = delimiter)
        return [i for i in csv_reader]

def write_2csv(csv_file, *lst):
    if lst:
        line = ",".join(lst)
        os.system("echo %s >> %s" % (line, csv_file))


def run_cmd(id, procedure, cmd, target, test, run_record, log = None):
    run_array = []
    if run_record:
        for line in read_csv(run_record):
            run_array.append("%s:%s:%s" % (line[0], line[1], line[2]))
    try:
        index = "%s:%s:%s" % (id, procedure, target)
        start_time = datetime.datetime.now()
        now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
        if not index in run_array:
            print("================= %s ====================\n%s\n" % (now, cmd))
            if not test:
                if log:
                    with open(log,'wb') as F:
                        p = subprocess.Popen(cmd, stdout = F, stderr = F, shell = True)
                        p.wait()
                else:
                    subprocess.check_output(cmd, shell = True)
                end_time   = datetime.datetime.now()
                cost_time  = str(end_time - start_time)
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time   = end_time.strftime("%Y-%m-%d %H:%M:%S")
                if run_record:
                    write_2csv(run_record, id, procedure, target, start_time, end_time, cost_time)
                print(index, "finished at", end_time )
    except subprocess.CalledProcessError as e:
        end_time   = datetime.datetime.now()
        end_time   = end_time.strftime("%Y-%m-%d %H:%M:%S")
        print(index, "error at", end_time )
    except Exception as ex:
        print("====== %s -=======" % ex)
        raise ex

class Pipeline():
    ''' pipeline to run '''
    def __init__(self, id, test = 1, run_record = None ):
        self.id         = id
        self.test       = test
        self.run_record = run_record
        self.pipeline   = collections.OrderedDict()

    def add_cmd(self, procedure, cmd, target, log = None):
        if self.pipeline.get(procedure, None):
            self.pipeline[procedure].append([cmd, target, log])
        else:
            self.pipeline[procedure] = [[cmd, target, log]]

    def run(self):
        for procedure in self.pipeline:
            cmd_tar_logs = self.pipeline[procedure]

            async_cnt = len(cmd_tar_logs)
            pool = Pool(async_cnt, init_worker,  maxtasksperchild = async_cnt)
            for cmd_tar_log in cmd_tar_logs:
                cmd, target, log = cmd_tar_log
                pool.apply_async(run_cmd,(self.id, procedure, cmd, target, self.test, self.run_record, log))
            pool.close()
            pool.join()
            pool.terminate()

def get_sys_mem_core():
    meminfo = open('/proc/meminfo').read()
    matched = re.search(r'^MemTotal:\s+(\d+)', meminfo)
    # meminfo.close()
    return (int(int(matched.groups()[0])/1024/1024), cpu_count())

def mkdir(*paths):
    for path in paths:
        if(os.path.exists(path)):
            pass
        else:
            os.system("mkdir -p %s" % path)

def mkcsv(file,*lst):
    if(os.path.isfile(file)):
        pass
    else:
        if lst:
            line = ".".join(lst)
            os.system("echo %s>%s" %(line,file))
        else:
            os.system("touch %s" % file)
