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


def return_cmd(cmd, print_only = False):
    try:
        if cmd:
            if print_only:
                print("==========================================\n%s\n" % cmd)
                return None
            else:
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

    def __init__(self, run_csv = None, test = 1, sync_cnt = sys_core):
        # run_array to record run status
        # for a run_csv, may be there are different ids
        self.run_csv   = run_csv
        self.run_array = {}
        self.test      = test
        self.sync_cnt  = sync_cnt
        self.pipelines = collections.OrderedDict()
        self.pool      = Pool(1)
        self.pool.terminate()
        if self.run_csv:
            if not os.path.exists(self.run_csv):
                os.system("echo 'id,procedure,target,start_time,end_time,cost_time' > %s" % self.run_csv)
            else:
                lines = read_csv(self.run_csv)
                # skip header
                lines = lines[1:]
                for line in lines:
                    id, procedure, target, start_time, end_time, cost_time = line.split(",")
                    index = "%s:%s:%s" % (id, procedure, target)
                    self.run_array[index] = "%s %s %s" % (start_time, end_time, cost_time)

    def append(self, id, procedure, cmd, target = None, log = None, run_sync = 0):
        # id 有可能会不同
        # 有些procedure, 是'无视'target,可以并行跑,用run_sync !=0 表示
        # 其实在一个procedure里，没有指明target，或者指向一致的情况下本来就是并行的
        # 但增加这个参数, 更明确.
        if run_sync:
            index = "%s:%s:" % (id, procedure)
        else:
            if target is None:
                index = "%s:%s:" % (id, procedure)
            else:
                index = "%s:%s:%s" % (id, procedure, target)
        if self.pipelines.get(index, None):
            self.pipelines[index].append([id, procedure, cmd, target, log])
        else:
            self.pipelines[index] = [[id, procedure, cmd, target, log]]

    def print_pipeline(self):
        for index in self.pipelines:
            pipeline = self.pipelines[index]
            for each in pipeline:
                id, procedure, cmd, target, log = each
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
                id, procedure, cmd, target, log = each
                self.pool.apply_async(Pipeline.run_cmd, args = (index, id, procedure, cmd, target, log, self.test, self.run_array, self.run_csv))
            self.pool.close()
            self.pool.join()
            self.pool.terminate()

    @staticmethod
    def run_cmd(index, id, procedure, cmd, target, log, test, run_array, run_csv):
        try:
            start_time = datetime.datetime.now()
            now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
            if index in run_array:
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
                        write_to_csv(run_csv, id, procedure, target, start_time_reform, end_time_reform, cost_time_reform)
                    print(index, "finished at", end_time_reform )
        except subprocess.CalledProcessError as e:
            end_time = datetime.datetime.now()
            end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
            print(index, "error at", end_time)
        except Exception as ex:
            traceback.print_exc()
            raise ex
# def run_cmd(id, procedure, cmd, target, run_csv, log = None):
    # run_array = []
    # try:
        # if run_csv:
            # for line in read_csv(run_csv):
                # run_array.append("%s:%s:%s" % (line[0], line[1], line[2]))
        # index = "%s:%s:%s" % (id, procedure, target)
        # start_time = datetime.datetime.now()
        # now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
        # if index in run_array:
            # pass
        # else:
            # print("================ %s ==================\n%s\n" % (now, cmd))
            # if log:
                # with open(log,'wb') as F:
                    # p = subprocess.Popen(cmd, stdout = F, stderr = F, shell = True)
                    # p.wait()
            # else:
                # subprocess.check_output(cmd, shell = True)
            # end_time   = datetime.datetime.now()
            # cost_time  = str(end_time - start_time)
            # start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
            # end_time   = end_time.strftime("%Y-%m-%d %H:%M:%S")
            # if run_csv:
                # write_2csv(run_csv, id,procedure, target, start_time, end_time, cost_time)
            # print(index, "finished at", end_time )
    # except subprocess.CalledProcessError as e:
        # end_time = datetime.datetime.now()
        # end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        # print(index, "error at", end_time)
    # except Exception as ex:
        # traceback.print_exc()
        # raise ex
