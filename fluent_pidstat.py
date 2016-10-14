#! /usr/bin/env python

# pip install docker-py fluent-logger python-daemon

import multiprocessing
import os.path
import re
import string
import subprocess
import sys
import time
from datetime import datetime

import daemon
import docker
from fluent import sender
from fluent import event

FLUENTD_HOST = "localhost"
FLUENTD_PORT = 24224
CMD_TEMPLATE = "pidstat ${p} -hurdw ${interval} 1"
INTERVAL_SECONDS = 10

def pidstat(cid):
    # CentOS7
    #tasks = open("/sys/fs/cgroup/systemd/system.slice/docker-%s.scope/tasks" % cid, 'r').readlines()

    # Ubuntu14
    tasks = open("/sys/fs/cgroup/cpuacct/docker/%s/tasks" % cid, 'r').readlines()

    pid_args = ""
    for pid in tasks:
        pid_args += " -p " + pid.rstrip()

        tmpl = string.Template(CMD_TEMPLATE)
        cmd = tmpl.substitute(p=pid_args, interval=INTERVAL_SECONDS)
    #print cmd

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out = [outline for outline in proc.communicate()[0].split('\n') if not outline.startswith('#')][1:]
    out = [out.strip() for out in out if len(out)]

    dataset = []
    for line in out:
        splittedline = re.split('\s{1,}', line)
        cmdline = " ".join(splittedline[18:])
        splittedline = splittedline[:18]
        splittedline.append(cmdline)
        dataset.append(splittedline)

    events = []
    for stat in dataset:
        tick = {}
        tick['Time'] = int(stat[0])
        #tick['UID'] = int(stat[1])
        tick['PID'] = int(stat[2])
        tick['usr'] = float(stat[3])
        tick['system'] = float(stat[4])
        #tick['guest'] = float(stat[5])
        tick['pCPU'] = float(stat[6])
        tick['CPU'] = int(stat[7])
        tick['minflt'] = float(stat[8])
        tick['majflt'] = float(stat[9])
        tick['VSZ'] = int(stat[10])
        tick['RSS'] = int(stat[11])
        tick['MEM'] = float(stat[12])
        tick['kB_rd'] = float(stat[13])
        tick['kB_wr'] = float(stat[14])
        tick['kB_ccwr'] = float(stat[15])
        tick['cswch'] = float(stat[16])
        tick['nvcswch'] = float(stat[17])
        #tick['Command'] = stat[18]

        try:
            cmdline = open("/proc/%d/cmdline" % tick['PID']).read()
        except IOError:
            continue

        if cmdline.count("thermos_executor") or cmdline.count("thermos_runner"):
            continue
        else:
            tick['Command'] = ' '.join(cmdline.split('\x00'))

        #print tick
        events.append(tick)

    for ev in events:
        event.Event(cid[:12], ev)

def daemon_task():
    cli = docker.Client()
    sender.setup('log.pidstat', host=FLUENTD_HOST, port=FLUENTD_PORT)

    while True:
        cids = []
        for c in cli.containers():
            image = c['Image']
            if image.count('docker_agent') or image.count('fluentd'):
                continue
            cids.append(c['Id'])

        if len(cids) == 0:
            time.sleep(INTERVAL_SECONDS)
            continue

        procs = []
        now = datetime.now()
        for cid in cids:
            print("%s CONTAINER ID:%s" % (now, cid[:12]))
            p = multiprocessing.Process(target=pidstat, args=(cid,))
            procs.append(p)
            p.start()

        [p.join() for p in procs]

# daemonize
if __name__ == '__main__':
    working_dir = os.path.abspath(os.path.dirname(__file__))

    context = daemon.DaemonContext(
        working_directory = working_dir,
        stdout = open("fluent_pidstat.out", "w+"),
        stderr = open("fluent_pidstat.err", "w+")
    )

    with context:
        daemon_task()

