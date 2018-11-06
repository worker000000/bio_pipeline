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
import hashlib
from multiprocessing import cpu_count
from multiprocessing import Pool

meminfo  = open('/proc/meminfo').read()
matched  = re.search(r'^MemTotal:\s+(\d+)', meminfo)
sys_mem  = int(int(matched.groups()[0])/1024/1024)
sys_core = cpu_count()

# 时间函数
def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def caluate_md5(file_full):
    try:
        myhash = hashlib.md5()
        with open(file_full,'rb') as f:
            while True:
                b = f.read(8096)
                if not b :
                    break
                myhash.update(b)
        return myhash.hexdigest()
    except Exception as e:
        raise e

def compare_md5(file_full,md5):
    try:
        if not os.path.isfile(file_full):
            raise Exception("%s is not a file" % file_full)
        if md5 :
            return (True if md5 == caluate_md5(file_full) else False)
        else:
            raise Exception("file or md5 absent")
    except Exception as e:
        print(file_full,e)
        return False

def check_md5infile(file_full,md5_file_full):
    try:
        fl = file_full.split("/")[-1]
        md5 = ""
        for line in os.popen("cat %s" % md5_file_full).readlines():
            ln = line.strip("\n")
            if fl == ln.split("  ")[1]:
                md5 = ln.split("  ")[0]
                break
        if md5:
            return compare_md5(file_full,md5)
        else:
            return False
    except Exception as e:
        print(e)
        return False


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
        os.system("mkdir -p %s" % path)


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

    def __init__(self, run_csv = None, sync_cnt = 2, test = 1, all_sync = False):
        # run_array to record run status
        # for a run_csv, may be there are different ids
        self.run_csv   = run_csv
        self.run_array = {}
        self.test      = test
        self.sync_cnt  = sync_cnt
        # One ID has its pipeline : pipelines[ID]
        self.pipelines = collections.OrderedDict()
        self.all_sync  = all_sync
        self.pool      = Pool(sync_cnt, init_worker, maxtasksperchild = sync_cnt)
        self.pool.terminate()
        if self.run_csv:
            if os.path.exists(self.run_csv):
                lines = read_csv(self.run_csv)
                # skip header
                lines = lines[1:]
                for line in lines:
                    ID, procedure, target, start_time, end_time, cost_time = line[:6]
                    runned = "%s:%s:%s" % (ID, procedure, target)
                    self.run_array[runned] = "%s %s %s" % (start_time, end_time, cost_time)
            else:
                os.system("echo 'ID,procedure,target,start_time,end_time,cost_time' > %s" % self.run_csv)

    def append(self, ID, procedure, cmd, target = None, log = None, run_sync = False):
        """
        有些procedure, 是'无视'target,可以并行跑
        如在一个procedure里，没有指明target，或者指向一致的情况下本来就是并行的
        增加run_sync, 可以在target不一致的情况下,并行运行.
        """
        if target is None or len(target.strip()) == 0 or run_sync:
            index = procedure
        else:
            index = "%s:%s" % (procedure, target)
        # 这里是关键，和一般的想法不同，是先考虑index，然后再考虑ID
        if self.pipelines.get(index, None) is None:
            self.pipelines[index] = collections.OrderedDict()
        if self.pipelines[index].get(ID, None):
            self.pipelines[index][ID].append([procedure, cmd, target, log])
        else:
            self.pipelines[index][ID] = [[procedure, cmd, target, log]]

    def print_pipeline(self):
        for ID in self.pipelines:
            pipeline = self.pipelines[ID]
            for index in pipeline:
                for each in pipeline[index]:
                    print(each)

    def run_pipeline(self):
        sync_cnt = len(self.pipelines)
        if sync_cnt > self.sync_cnt:
            sync_cnt = self.sync_cnt
        self.pool = Pool(sync_cnt, init_worker, maxtasksperchild = sync_cnt)
        for index in self.pipelines:
            pipeline = self.pipelines[index]
            # syn_cnt is the cnt of functions run async in a pipelines
            for ID in pipeline:
                for each in pipeline[ID]:
                    procedure, cmd, target, log = each
                    self.pool.apply_async(Pipeline.run_cmd, args = (ID, procedure, cmd, target, log, self.test, self.run_array, self.run_csv))
        self.pool.close()
        self.pool.join()
        self.pool.terminate()

    @staticmethod
    def run_cmd(ID, procedure, cmd, target, log, test, run_array, run_csv):
        try:
            start_time = datetime.datetime.now()
            now        = start_time.strftime("%Y-%m-%d %H:%M:%S")
            if target is None or len(target) == 0:
                target = ""
            runned     = "%s:%s:%s" % (ID, procedure, target)
            if runned in run_array:
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
                    print(runned, "finished at", end_time_reform)
        except subprocess.CalledProcessError as ex:
            end_time        = datetime.datetime.now()
            end_time_reform = end_time.strftime("%Y-%m-%d %H:%M:%S")
            print(runned, "error at", end_time_reform, ex)
        except Exception as ex:
            traceback.print_exc()
            raise ex
