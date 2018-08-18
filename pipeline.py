# coding:utf8
'''Author leoatchina'''
import os
import traceback
import collections
import signal
import csv
import re
import datetime
import subprocess
from multiprocessing import cpu_count
from multiprocessing import Pool

meminfo  = open('/proc/meminfo').read()
matched  = re.search(r'^MemTotal:\s+(\d+)', meminfo)
sys_mem  = int(int(matched.groups()[0])/1024/1024)
sys_core = cpu_count()


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def return_cmd(cmd):
    try:
        if cmd:
            tmp = os.popen(cmd).readlines()
            return [line.strip() for line in tmp]
        else:
            raise Exception("cmd is None")
    except Exception as e:
        print(e)
        return None


def mkdirs(*paths):
    for path in paths:
        os.makedirs(path)


def write_to_csv(csv, *lst):
    if lst:
        lst = [ i if len(i) > 0 else "" for i in lst  ]
        line = ",".join(lst)
        os.system("echo %s >> %s" % (line, csv))


def read_csv(csv_file, delimiter=","):
    with open(csv_file) as csv_handle:
        csv_reader = csv.reader(csv_handle, delimiter = delimiter)
        return [i for i in csv_reader]


class Pipeline(object):
    ''' pipelines to run '''
    def __del__(self):
        self.pool.terminate()

    def __init__(self, run_csv = None, sync_cnt = 2, test = 1):
        # run_array to record run status
        # for a run_csv, may be there are different ids
        self.run_csv   = run_csv
        self.run_array = {}
        self.test      = test
        self.sync_cnt  = sync_cnt
        self.pipelines = collections.OrderedDict()
        self.pool      = Pool(self.sync_cnt)
        self.pool.terminate()
        if self.run_csv:
            if os.path.exists(self.run_csv):
                lines = read_csv(self.run_csv)
                # skip header
                lines = lines[1:]
                for line in lines:
                    ID, procedure, target, start_time, end_time, cost_time = line
                    index = "%s:%s:%s" % (ID, procedure, target)
                    self.run_array[index] = "%s %s %s" % (start_time, end_time, cost_time)
            else:
                os.system("echo 'ID,procedure,target,start_time,end_time,cost_time' > %s" % self.run_csv)

    def append(self, ID, procedure, cmd, target = None, log = None, run_sync = 0):
        # 有些procedure, 是'无视'target,可以并行跑
        # 如在一个procedure里，没有指明target，或者指向一致的情况下本来就是并行的
        # 增加run_sync, 可以在target不一致的情况下,并行运行.
        # 通过构建index的方式, 一个pipeline是一个list,可以并行执行
        if run_sync:
            index = "%s:%s:" % (ID, procedure)
        else:
            if not len(target):
                index = "%s:%s:" % (ID, procedure)
            else:
                index = "%s:%s:%s" % (ID, procedure, target)
        if self.pipelines.get(index, None):
            self.pipelines[index].append([ID, procedure, cmd, target, log])
        else:
            self.pipelines[index] = [[ID, procedure, cmd, target, log]]

    def print_pipeline(self):
        for index in self.pipelines:
            pipeline = self.pipelines[index]
            for each in pipeline:
                ID, procedure, cmd, target, log = each
                print("===============================\n%s\n\n%s" % (index, cmd))

    def run_pipeline(self):
        for index in self.pipelines:
            pipeline = self.pipelines[index]
            # syn_cnt is the cnt of functions run async in a pipelines
            sync_cnt = len(pipeline)
            if sync_cnt > self.sync_cnt:
                sync_cnt = self.sync_cnt
            self.pool = Pool(sync_cnt, init_worker, maxtasksperchild = sync_cnt)
            for each in pipeline:
                ID, procedure, cmd, target, log = each
                self.pool.apply_async(Pipeline.run_cmd, args = (ID, procedure, cmd, target, log, self.test, self.run_array, self.run_csv))
            self.pool.close()
            self.pool.join()
            self.pool.terminate()

    @staticmethod
    def run_cmd(ID, procedure, cmd, target, log, test, run_array, run_csv):
        try:
            start_time = datetime.datetime.now()
            now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
            if not len(target):
                target = ""
            ruuned     = "%s:%s:%s" % (ID, procedure, target)
            if ruuned in run_array:
                pass
            else:
                print("================ %s ===============\n%s\n" % (now, cmd))
                if not test:
                    if log:
                        with open(log, 'wb') as file_out:
                            p = subprocess.Popen(cmd, stdout = file_out, stderr = file_out, shell = True)
                            p.wait()
                    else:
                        subprocess.check_output(cmd, shell = True)
                    end_time          = datetime.datetime.now()
                    cost_time_reform  = str(end_time - start_time)
                    start_time_reform = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    end_time_reform   = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    if run_csv:
                        write_to_csv(run_csv, ID, procedure, target, start_time_reform, end_time_reform, cost_time_reform)
                    print(runned, "finished at", end_time_reform )
        except subprocess.CalledProcessError as e:
            end_time        = datetime.datetime.now()
            end_time_reform = end_time.strftime("%Y-%m-%d %H:%M:%S")
            print(runned, "error at", end_time_reform)
        except Exception as ex:
            traceback.print_exc()
            raise ex
