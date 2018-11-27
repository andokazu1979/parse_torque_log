#!/usr/bin/env python

import os
import sys
import glob
import datetime
import pandas as pd
from pandas import Series, DataFrame

########################################
# User settings
########################################

DIRPATH_LOG = '/var/spool/torque/server_priv/accounting'
PERIOD_STA = '20170401'

########################################
# Main
########################################

#---------------------------------------
# Parse log files and construct dataframe
#---------------------------------------

d = {'user' : [], 'group' : [], 'queue' : [], 'nnodes' : [], 'mem' : [], 'vmem' : [], 'walltime' : [], 'node_hour' : []}
for fpath in glob.glob(os.path.join(DIRPATH_LOG, '????????')):
    df_all_records = pd.read_table(fpath, header=None, sep=';', names=['date', 'marker', 'jobid', 'others'])
    df_all_records['date'] = df_all_records['date'].map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y %H:%M:%S'))
    for i in df_all_records[df_all_records['date'] >= datetime.datetime.strptime(PERIOD_STA, '%Y%m%d')].index:
        rec = df_all_records.loc[i]
        if rec.marker == 'E' and 'resources_used' in rec.others:
            user = ''
            group = ''
            queue = ''
            nnodes = None
            mem = None
            vmem = None
            walltime = None
            for item in rec.others.split():
                if 'user' in item:
                    user = item.split('=')[1]
                    d['user'].append(user)
                elif 'queue' in item:
                    queue = item.split('=')[1]
                    d['queue'].append(queue)
                elif 'group' in item:
                    group = item.split('=')[1]
                    d['group'].append(group)
                elif 'unique_node_count' in item:
                    nnodes = int(item.split('=')[1])
                    d['nnodes'].append(nnodes)
                elif 'resources_used.mem' in item:
                    mem = int(item.split('=')[1].replace('kb', ''))
                    d['mem'].append(mem)
                elif 'resources_used.vmem' in item:
                    vmem = int(item.split('=')[1].replace('kb', ''))
                    d['vmem'].append(vmem)
                elif 'resources_used.walltime' in item:
                    walltime = int(item.split('=')[1])
                    d['walltime'].append(walltime)
            d['node_hour'].append(int(nnodes * (float(walltime) / 3600)))
    df_exit = DataFrame(d)

#---------------------------------------
# Fix up and show dataframe
#---------------------------------------

s_node_hour = df_exit.groupby([df_exit['group'], df_exit['user']]).sum()['node_hour']
s_node_njobs = df_exit.groupby([df_exit['group'], df_exit['user']]).size()
s_node_njobs.name = "njobs"
print(pd.concat([s_node_hour, s_node_njobs], axis=1))
