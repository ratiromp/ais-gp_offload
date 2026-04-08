#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
import logging
import threading
import subprocess
import tempfile
import uuid
import json
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
from Queue import Queue
from collections import OrderedDict

try:
    long
except NameError:
    long = int

# ==============================================================================
# 1. Utilities: Tracker & Monitor Dashboard
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, execution_id, log_path, base_output_sh):
        self.execution_id = execution_id
        self.total_task = 0
        self.log_path = log_path
        self.base_output_sh = base_output_sh
        self.completed_task = 0
        self.results = []
        self.current_action = "Initializing..."
        self.start_time = time.time()
        self.lock = threading.Lock()

    def update_action(self, action_msg):
        with self.lock:
            self.current_action = action_msg

    def add_result(self, table_name, status, path, rec_start_ts, sh_filename, remark="-"):
        with self.lock:
            self.results.append({
                'table': table_name,
                'status': status,
                'path': path,
                'rec_start_ts': rec_start_ts,
                'sh_filename': sh_filename,
                'remark': str(remark),
                'end_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            self.completed_task += 1

    def get_dashboard_data(self):
        with self.lock:
            return self.completed_task, self.total_task, self.current_action

    def print_summary(self, logger):
        from collections import OrderedDict
        script_stats = OrderedDict()
        for r in self.results:
            fn = r['sh_filename']
            if fn != '-':
                script_stats[fn] = script_stats.get(fn, 0) + 1
        
        sorted_filenames = sorted(script_stats.keys())
        out_dir = os.path.dirname(self.base_output_sh)

        total_added_to_purge = sum(1 for r in self.results if r.get('sh_filename', '-') != '-')
        skip_count = sum(1 for r in self.results if r.get('status') == 'PURGE_SKIPPED')
        success_count = sum(1 for r in self.results if r.get('status') in ['PURGE_SUCCESS', 'ADD_TO_PURGE_LIST', 'FORCE_PURGE'])
        
        elapsed_secs = int(time.time() - self.start_time)
        h, rem = divmod(elapsed_secs, 3600)
        m, s = divmod(rem, 60)
        duration_str = "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)

        summary_lines = [
            "",
            "="*90,
            " FINAL SUMMARY REPORT : {0}".format(self.execution_id),
            "="*90,
            " Total Tables Processed : {0}".format(len(self.results)),
            " Added to Deletion List : {0} (Includes FORCE/DRY-RUN Mode)".format(total_added_to_purge),
            " Successfully Deleted   : {0}".format(success_count),
            " Skipped                : {0}".format(skip_count),
            " Duration               : {0}".format(duration_str),
            "-"*90,
            " Log File               : {0}".format(self.log_path)
        ]
        
        if not sorted_filenames:
            summary_lines.append(" Output Script          : - (No scripts generated)")
        elif len(sorted_filenames) <= 4:
            for idx, fn in enumerate(sorted_filenames, 1):
                full_path = os.path.join(out_dir, fn)
                summary_lines.append(" Output Script {0:<9} : {1} ({2} tables)".format(idx, full_path, script_stats[fn]))
        else:
            for idx in [1, 2]:
                fn = sorted_filenames[idx-1]
                full_path = os.path.join(out_dir, fn)
                summary_lines.append(" Output Script {0:<9} : {1} ({2} tables)".format(idx, full_path, script_stats[fn]))
            summary_lines.append(" ...")
            last_idx = len(sorted_filenames)
            fn = sorted_filenames[-1]
            full_path = os.path.join(out_dir, fn)
            summary_lines.append(" Output Script {0:<9} : {1} ({2} tables)".format(last_idx, full_path, script_stats[fn]))
        
        summary_lines.append("="*90)
        for line in summary_lines:
            logger.info(line)
            print(line)

class MonitorThread(threading.Thread):
    def __init__(self, tracker):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard() # Print one last time before exiting

    def print_dashboard(self):
        script_stats = {}
        for r in self.tracker.results:
            fn = r['sh_filename']
            if fn != '-':
                script_stats[fn] = script_stats.get(fn, 0) + 1
        
        sorted_filenames = sorted(script_stats.keys())
        out_dir = os.path.dirname(self.tracker.base_output_sh)
        comp, total, action = self.tracker.get_dashboard_data()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        success_count = sum(1 for r in self.tracker.results if r.get('status') in ['PURGE_SUCCESS', 'ADD_TO_PURGE_LIST', 'FORCE_PURGE'])

        lines = [
            "="*120,
            " HDFS HOUSEKEEPING MONITOR (Python 2.7) ",
            " Execution ID: {0}".format(self.tracker.execution_id),
            "="*120,
            " Progress: {0}/{1} ({2:.2f}%) | Success: {3}".format(comp, total, pct, success_count),
            " Elapsed : {0:.0f}s".format(elapsed),
            " Action  : {0}".format(action)[:119],
            "-"*120,
            " Log Path: {0}".format(self.tracker.log_path)
        ]

        if not sorted_filenames:
            lines.append(" Out Files: -")
        elif len(sorted_filenames) <= 4:
            for idx, fn in enumerate(sorted_filenames, 1):
                full_path = os.path.join(out_dir, fn)
                lines.append(" Output Script {0:<3} : {1} ({2} tables)".format(idx, full_path, script_stats[fn]))
        else:
            fn_first = sorted_filenames[0]
            lines.append(" Output Script 1   : {0} ({1} tables)".format(os.path.join(out_dir, fn_first), script_stats[fn_first]))
            lines.append(" ...")
            fn_last = sorted_filenames[-1]
            last_idx = len(sorted_filenames)
            lines.append(" Output Script {0:<3} : {1} ({2} tables)".format(last_idx, os.path.join(out_dir, fn_last), script_stats[fn_last]))

        lines.append("="*120)

        if not getattr(self, 'first_print', True):
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        sys.stdout.write(output)
        sys.stdout.flush()

# ==============================================================================
# 2. Config & Logging
# ==============================================================================

class HousekeepingLogger(object):
    @staticmethod
    def setup(log_dir, global_ts):
        try:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_file = os.path.join(log_dir, "housekeeping_{0}.log".format(global_ts))
            
            logger = logging.getLogger("HousekeepingJob")
            logger.setLevel(logging.INFO)
            logger.handlers = []
            
            fh = logging.FileHandler(log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            logger.addHandler(fh)
            
            return logger, log_file
        except Exception as e:
            print("CRITICAL ERROR: Cannot create log directory/file. Error: {0}".format(e))
            sys.exit(1)

class ConfigManager(object):
    def __init__(self, logger, project_root):
        self.logger = logger
        self.project_root = project_root
        self.args = None
        self.hdfs_prefix = ""
        self.retention_hours = 24.0
        self.kinit_cmd = ""
        self.beeline_url = ""
        self.beeline_user = ""
        self.beeline_pwd = ""
        self.total_sh_files = 4
        self.check_hdfs_path_concurrency = 4
        self.execute_sh_concurrency = 4
        self.conda_activate_cmd = ""
        self.parquet_writer_path = ""
        self.audit_table_hdfs_path = ""
        self.hive_log_table = ""
        self.pending_purge_vw_name = ""

    def parse_args(self):
        parser = argparse.ArgumentParser(description="HDFS Parquet Housekeeping")
        parser.add_argument('--config', default='env_config.txt', help='Path or filename for env_config.txt')
        parser.add_argument('--force', action='store_true', help='Bypass retention age check')
        parser.add_argument('--dry-run', action='store_true', help='Simulate execution without deleting')
        self.args = parser.parse_args()

        config_dir = os.path.join(self.project_root, 'config')

        if not os.path.isabs(self.args.config):
            self.args.config = os.path.join(config_dir, self.args.config)

    def load_configs(self):
        self.logger.info("--- [1/2] Input Parameters ---")
        self.logger.info("Config File : {0}".format(self.args.config))
        self.logger.info("Force Mode  : {0}".format(self.args.force))
        self.logger.info("Dry-Run Mode: {0}".format(self.args.dry_run))
        self.logger.info("------------------------------")

        # 1. Load env_config.txt (Remains the same)
        try:
            with open(self.args.config, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, val = [x.strip() for x in line.split('=', 1)]
                        if key == 'hdfs_prefix': self.hdfs_prefix = val
                        elif key == 'retention_hours': self.retention_hours = float(val)
                        elif key == 'kinit': self.kinit_cmd = val
                        elif key == 'beeline_url': self.beeline_url = val
                        elif key == 'beeline_user': self.beeline_user = val
                        elif key == 'beeline_pwd': self.beeline_pwd = val
                        elif key == 'check_hdfs_path_concurrency': self.check_hdfs_path_concurrency = int(val)
                        elif key == 'total_sh_files': self.total_sh_files = int(val)
                        elif key == 'execute_sh_concurrency': self.execute_sh_concurrency = int(val)
                        elif key == 'parquet_writer_path': self.parquet_writer_path = val
                        elif key == 'conda_activate_cmd': self.conda_activate_cmd = val
                        elif key == 'hive_log_table': self.hive_log_table = val
                        elif key == 'pending_purge_vw_name': self.pending_purge_vw_name = val
            self.logger.info("--- [2/2] Environment Config ---")
            self.logger.info("HDFS Prefix               : {0}".format(self.hdfs_prefix))
            self.logger.info("Retention Hours           : {0}".format(self.retention_hours))
            self.logger.info("Check HDFS Concurrency    : {0}".format(self.check_hdfs_path_concurrency))
            self.logger.info("Target SH Files           : {0}".format(self.total_sh_files))
            self.logger.info("Execute Shell Concurrency : {0}".format(self.execute_sh_concurrency))
            self.logger.info("parquet_writer_path       : {0}".format(self.parquet_writer_path))
            self.logger.info("hive_log_table            : {0}".format(self.hive_log_table))
            self.logger.info("pending_purge_vw_name     : {0}".format(self.pending_purge_vw_name))
            self.logger.info("Kinit Command             : {0}".format("Found" if self.kinit_cmd else "None"))
            self.logger.info("--------------------------------")
            if self.kinit_cmd:
                self.logger.info("Loaded Config -> Kinit Command: Found")
            if not self.beeline_url: raise ValueError("beeline_url is missing in config.")
            if not self.parquet_writer_path: raise ValueError("parquet_writer_path is missing in config.")
        except Exception as e:
            self.logger.critical("Failed to read config file: {0}".format(e))
            raise

# ==============================================================================
# Phase 0 - Input Fetching
# ==============================================================================
class HiveQueryHelper(object):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def fetch_pending_housekeeping(self):
        self.logger.info("Phase 0: Fetching pending tables from Hive via Pushdown Query...")
        
        # Predicate Pushdown Logic
        retention_sec = int(self.config.retention_hours * 3600)
        force_logic = "1=1" if self.config.args.force else "(UNIX_TIMESTAMP() - UNIX_TIMESTAMP(rec_start_ts)) >= {0}".format(retention_sec)

        query = """
        SELECT table_name, rec_start_ts
        FROM {0}
        WHERE {1}
        """.format(self.config.pending_purge_vw_name, force_logic)

        cmd = [
            'beeline',
            '-u', '"{0}"'.format(self.config.beeline_url),
            '-n', self.config.beeline_user,
            '-p', self.config.beeline_pwd,
            '--outputformat=tsv2',
            '--showHeader=false',
            '-e', '"{0}"'.format(query)
        ]

        safe_cmd = " ".join(cmd).replace(self.config.beeline_pwd, '***')
        self.logger.info("Executing Beeline: {0}".format(safe_cmd))

        table_list = []
        try:
            # Using shell=True and joining cmd for complex jdbc string handling
            output = subprocess.check_output(" ".join(cmd), shell=True, stderr=subprocess.STDOUT)
            lines = output.decode('utf-8', 'ignore').strip().split('\n')
            
            for line in lines:
                if not line.strip(): continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    raw_name = parts[0].strip()
                    rec_start_ts = parts[1].strip()
                    
                    # Support db.schema.table format (3 parts)
                    name_parts = raw_name.split('.')
                    if len(name_parts) >= 3:
                        # db.schema.table -> db/schema/table
                        nested_path = "{0}/{1}/{2}".format(name_parts[0].strip(), name_parts[1].strip(), name_parts[2].strip())
                    elif len(name_parts) == 2:
                        # db.table -> db/table
                        nested_path = "{0}/{1}".format(name_parts[0].strip(), name_parts[1].strip())
                    else:
                        # Fallback for unexpected format
                        nested_path = raw_name.replace('.', '/')
                    
                    table_list.append({
                        'raw_name': raw_name,
                        'nested_path': nested_path,
                        'rec_start_ts': rec_start_ts
                    })
            
            self.logger.info("Found {0} tables ready for housekeeping.".format(len(table_list)))
            return table_list
            
        except subprocess.CalledProcessError as e:
            error_msg = e.output.decode('utf-8', 'ignore') if e.output else str(e)
            self.logger.critical("Hive Query Failed: {0}".format(error_msg))
            raise

# ==============================================================================
# Phase 1 - Parallel Validation & Chunked Script Generation
# ==============================================================================

class CommandGenerator(object):
    def __init__(self, config, tracker, logger):
        self.config = config
        self.tracker = tracker
        self.logger = logger
        self.generated_scripts = []

    def sanitize_path(self, prefix, nested_name):
        p = prefix.replace('\\', '/').rstrip('/')
        t = nested_name.replace('\\', '/').lstrip('/')
        return "{0}/{1}".format(p, t)

    def _worker_check_path(self, item):
        """ ThreadPool worker for Parallel HDFS testing """
        hdfs_path = self.sanitize_path(self.config.hdfs_prefix, item['nested_path'])
        item['target_hdfs_path'] = hdfs_path
        
        cmd = 'hdfs dfs -test -e {0}'.format(hdfs_path)
        try:
            item['path_exists'] = (subprocess.call(cmd, shell=True) == 0)
        except Exception:
            item['path_exists'] = False
        return item

    def write_command_block(self, file_obj, table_name, hdfs_path):
        file_obj.write('echo "=================================================="\n')
        file_obj.write('echo "[START] {0} : {1}"\n'.format(table_name, hdfs_path))
        file_obj.write('echo "=================================================="\n')
        file_obj.write('hdfs dfs -rm -f {0}/*/*.parquet || exit 1\n'.format(hdfs_path))
        file_obj.write('hdfs dfs -rm -f {0}/*.parquet || exit 1\n'.format(hdfs_path))
        file_obj.write('hdfs dfs -rmdir {0} || exit 1\n'.format(hdfs_path))
        file_obj.write('echo "[SUCCESS] {0} deleted."\n\n'.format(table_name))

    def generate(self, table_list, base_sh_path):
        self.logger.info("Phase 1: Validating HDFS Paths in Parallel...")
        
        # 1. Parallel Validation
        check_concurrency = self.config.check_hdfs_path_concurrency
        pool = ThreadPool(check_concurrency)
        validated_list = pool.map(self._worker_check_path, table_list)
        pool.close()
        pool.join()

        self.logger.info("Phase 1: Generating chunked scripts...")
        
        valid_tables = []
        for item in validated_list:
            table = item['raw_name']
            if not item['path_exists']:
                self.logger.info("[SKIP] {0} - Path Not Found".format(table))
                self.tracker.add_result(
                    table_name=table, 
                    status='PURGE_SKIPPED', 
                    path=item['target_hdfs_path'], 
                    rec_start_ts=item['rec_start_ts'], 
                    sh_filename="-", 
                    remark="Path Not Found" 
                )
            else:
                valid_tables.append(item)

        if not valid_tables:
            self.logger.info("No valid HDFS paths found to generate scripts.")
            return

        target_files = min(self.config.total_sh_files, len(valid_tables))
        k, m = divmod(len(valid_tables), target_files)
        
        chunks = [valid_tables[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(target_files)]
        
        try:
            for chunk_idx, chunk_items in enumerate(chunks, 1):
                sh_name = base_sh_path.replace('.sh', '_p{0}.sh'.format(chunk_idx))
                self.generated_scripts.append(sh_name)
                
                with open(sh_name, 'w') as current_file:
                    current_file.write("#!/bin/bash\n# Housekeeping Chunk {0}\n\n".format(chunk_idx))
                    
                    for item in chunk_items:
                        table = item['raw_name']
                        target_path = item['target_hdfs_path']
                        rec_start_ts = item['rec_start_ts']
                        
                        self.tracker.update_action("Processing: {0}".format(table))
                        self.write_command_block(current_file, table, target_path)
                        
                        sh_filename_only = os.path.basename(sh_name)
                        action_mode = 'DRY_RUN_MODE' if self.config.args.dry_run else ('FORCE_PURGE' if self.config.args.force else 'ADD_TO_PURGE_LIST')
                        
                        self.logger.info("[{0}] {1} -> {2}".format(action_mode, table, sh_filename_only))
                        self.tracker.add_result(table, action_mode, target_path, rec_start_ts, sh_filename_only)

        except Exception as e:
            self.logger.error("Script generation failed: {0}".format(e), exc_info=True)
            raise

# ==============================================================================
# Phase 2 - Parallel Execution & Log Buffer
# ==============================================================================

class ParallelExecutor(object):
    def __init__(self, config, tracker, logger):
        self.config = config
        self.tracker = tracker
        self.logger = logger
        self.concurrency = self.config.execute_sh_concurrency

    def worker(self, queue):
        while not queue.empty():
            sh_path = queue.get()
            sh_name = os.path.basename(sh_path)
            self.logger.info("[THREAD] Starting execution of {0}".format(sh_name))
            
            try:
                # Capture buffered output
                process = subprocess.Popen(['bash', sh_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = process.communicate()
                
                # Flush buffer to log file atomically
                if process.returncode != 0:
                    self.logger.error("[SHELL_ERROR] {0} failed.\n>> STDOUT:\n{1}\n>> STDERR:\n{2}".format(sh_name, out, err))
                    # Mark corresponding tables as ERROR in tracker
                    self._update_tracker_status(sh_name, 'PURGE_FAILED', err)
                else:
                    self.logger.info("[SHELL_SUCCESS] {0} completed.\n>> STDOUT:\n{1}".format(sh_name, out))
                    self._update_tracker_status(sh_name, 'PURGE_SUCCESS', 'Purged successfully')
            except Exception as e:
                self.logger.error("[THREAD_ERROR] Exception in {0}: {1}".format(sh_name, e))
            finally:
                queue.task_done()

    def _update_tracker_status(self, sh_name, new_status, remark):
        with self.tracker.lock:
            for r in self.tracker.results:
                if r['sh_filename'] == sh_name and r['status'] in ['ADD_TO_PURGE_LIST', 'FORCE_PURGE']:
                    r['status'] = new_status
                    r['remark'] = str(remark)[:200] # Truncate remark

    def execute_all(self, script_list):
        self.logger.info("Phase 2: Executing {0} scripts with concurrency {1}...".format(len(script_list), self.concurrency))
        
        queue = Queue()
        for sh in script_list:
            queue.put(sh)
            
        threads = []
        for _ in range(min(self.concurrency, len(script_list))):
            t = threading.Thread(target=self.worker, args=(queue,))
            t.daemon = True
            t.start()
            threads.append(t)
            
        queue.join()
        self.logger.info("Phase 2: All parallel executions completed.")

# ==============================================================================
# Main Controller & Phase 3
# ==============================================================================
class AuditLogManager(object):
    def __init__(self, config, tracker, logger, global_ts, global_date, start_datetime, execution_id, temp_dir):
        self.config = config
        self.tracker = tracker
        self.logger = logger
        self.global_ts = global_ts
        self.global_date = global_date
        self.start_datetime = start_datetime
        self.execution_id = execution_id
        self.temp_dir = temp_dir

    def _calculate_age_str(self, rec_start_ts_str):
        try:
            rec_ts = datetime.strptime(rec_start_ts_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            age_secs = (datetime.now() - rec_ts).total_seconds()
            age_hours = age_secs / 3600.0
            return "{0:.2f}h".format(age_hours)
        except Exception:
            return "-"

    def write_audit_log(self):
        self.logger.info("Phase 3: Writing Audit Log to Hive...")
        audit_data = []
        
        for res in self.tracker.results:
            audit_data.append({
                "table_name": res['table'],
                "action_mode": res['status'],
                "target_hdfs_path": res.get('path', ''),
                "last_modified": res.get('rec_start_ts', ''),
                "retention_criteria": "{0}h".format(self.config.retention_hours),
                "age": self._calculate_age_str(res.get('rec_start_ts', '')),
                "sh_file_name": res.get('sh_filename', '-'),
                "status": "PURGE_SUCCESS" if res['status'] in ['ADD_TO_PURGE_LIST', 'FORCE_PURGE'] else res['status'],
                "remark": res.get('remark', '-'),
                "start_ts": self.start_datetime,
                "end_ts": res.get('end_ts', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                "execution_id": self.execution_id
            })

        final_payload = {
            "status": {"job_status": "SUCCESS"},
            "result": audit_data
        }

        result_dir = os.path.join(self.temp_dir, 'result')
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        temp_json = os.path.join(self.temp_dir, "hk_audit_{0}_{1}.json".format(self.global_ts, self.execution_id))
        with open(temp_json, 'w') as f:
            json.dump(final_payload, f)
            
        conda_cmd = "{0} && python {1} {2} {3} {4}".format(
            self.config.conda_activate_cmd, 
            self.config.parquet_writer_path, 
            temp_json,       # arg 1: json_payload_path
            self.temp_dir,   # arg 2: status_dir
            result_dir    # arg 3: result_dir
        )
        
        try:
            self.logger.info("Converting JSON to Parquet...")
            proc = subprocess.Popen(
                conda_cmd, 
                shell=True, 
                executable='/bin/bash',
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            stdout, stderr = proc.communicate()
            
            if proc.returncode != 0:
                self.logger.error("Parquet Writer Failed (RC={0})".format(proc.returncode))
                self.logger.error("STDOUT: {0}".format(stdout))
                self.logger.error("STDERR: {0}".format(stderr))
                raise subprocess.CalledProcessError(proc.returncode, conda_cmd)
            
            self.logger.info("Parquet conversion successful.")
            
            hdfs_target_path = self.config.hive_log_table
            subprocess.call("hdfs dfs -mkdir -p {0}".format(hdfs_target_path), shell=True)
            move_cmd = "hdfs dfs -moveFromLocal -f {0}/part_*.parquet {1}/".format(
                result_dir, hdfs_target_path
            )
            subprocess.check_call(move_cmd, shell=True)
            
            self.logger.info("Audit log written to HDFS successfully.")
            
        except Exception as e:
            self.logger.error("Failed to write Parquet Audit Log: {0}".format(e), exc_info=True)
        finally:
            if os.path.exists(temp_json):
                os.remove(temp_json)

class HousekeepingJob(object):
    def __init__(self):
        self.global_date = datetime.now().strftime("%Y%m%d")
        self.global_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.short_uuid = str(uuid.uuid4()).split('-')[0]
        self.execution_id = "JOB-HK_{0}_{1}".format(self.global_ts, self.short_uuid)
        
        # Setup paths
        self.main_path = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(self.main_path)
        self.log_dir = os.path.join(self.project_root, 'log', self.global_date)
        self.output_dir = os.path.join(self.project_root, 'output', self.global_date)
        self.temp_dir = os.path.join(self.project_root, 'temp')

        for d in [self.output_dir, self.log_dir, self.temp_dir]:
            if not os.path.exists(d):
                try:
                    os.makedirs(d)
                except OSError as e:
                    print("CRITICAL ERROR: Cannot create directory {0}: {1}".format(d, e))
                    sys.exit(1)

        self.base_output_sh = os.path.join(
            self.output_dir, 
            "execute_housekeeping_{0}_{1}.sh".format(self.global_ts, self.short_uuid)
        )

        # Init Logger
        self.logger, self.log_path = HousekeepingLogger.setup(self.log_dir, self.global_ts)
        self.logger.info("Initializing Housekeeping Job ID: {0}".format(self.execution_id))

        # Init Config
        self.config = ConfigManager(self.logger, self.project_root)
        self.config.parse_args()
        self.config.load_configs()

        if self.config.kinit_cmd:
            self._authenticate_kerberos()

        # Init Trackers & Handlers
        self.tracker = ProcessTracker(self.execution_id, self.log_path, self.base_output_sh)
        self.hive_helper = HiveQueryHelper(self.config, self.logger)
        self.generator = CommandGenerator(self.config, self.tracker, self.logger)
        self.executor = ParallelExecutor(self.config, self.tracker, self.logger)

        self.audit_manager = AuditLogManager(
            self.config, self.tracker, self.logger, 
            self.global_ts, self.global_date, self.start_datetime, self.execution_id,
            self.temp_dir
        )

    def _authenticate_kerberos(self):
        max_retries = 2
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info("Executing Kerberos authentication (Attempt {0}/{1})...".format(attempt, max_retries))
                subprocess.check_output(self.config.kinit_cmd, shell=True, stderr=subprocess.STDOUT)
                self.logger.info("Kerberos authentication successful.")
                return
            except subprocess.CalledProcessError as e:
                # Python 2.7 compatible decode with ignore
                error_msg = e.output.decode('utf-8', 'ignore') if e.output else str(e)
                if attempt < max_retries:
                    self.logger.warning("Kerberos authentication failed on attempt {0}. Retrying... Error: {1}".format(attempt, error_msg))
                    time.sleep(1)
                else:
                    self.logger.critical("Kerberos authentication FAILED after {0} attempts. HDFS commands will fail. \n>> ACTUAL ERROR: {1}".format(max_retries, error_msg))
                    raise Exception("Kerberos Authentication Failed: {0}".format(error_msg))

    def run(self):
        # 1. Start Dashboard Monitor
        monitor = MonitorThread(self.tracker)
        monitor.start()

        try:
            # Phase 0
            target_tables = self.hive_helper.fetch_pending_housekeeping()
            self.tracker.total_task = len(target_tables)
            
            if not target_tables:
                self.tracker.update_action("No pending tables found. Exiting.")
            else:
                # Phase 1
                self.generator.generate(target_tables, self.base_output_sh)
                self.tracker.update_action("Script Generation Completed.")

                # Phase 2
                if self.config.args.dry_run:
                    self.tracker.update_action("DRY-RUN mode. Execution skipped.")
                    self.logger.info("Dry-Run mode active. Skipping Phase 2 Execution.")
                elif self.generator.generated_scripts:
                    self.tracker.update_action("Executing shell scripts (Parallel)...")
                    self.executor.execute_all(self.generator.generated_scripts)

            # Phase 3
            if self.tracker.results:
                self.tracker.update_action("Writing Audit Log...")
                self.audit_manager.write_audit_log()

        except KeyboardInterrupt:
            self.logger.warning("Job Interrupted by User!")
        except Exception as e:
            self.logger.error("Job Aborted due to fatal error.", exc_info=True)
        finally:
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.logger)
            self.logger.info("Job Finished.")

if __name__ == "__main__":
    try:
        job = HousekeepingJob()
        job.run()
    except Exception as e:
        print("FATAL INIT ERROR: {0}".format(e))
        sys.exit(1)