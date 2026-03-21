#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import csv
import time
import argparse
import logging
import threading
import Queue
import glob
import subprocess
import tempfile
import uuid
from datetime import datetime

# ==============================================================================
# 1. Utilities: Logger, ProcessTracker & Monitor
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, logger):
        self.logger = logger
        self.lock = threading.Lock()
        self.results = []
        self.worker_status = {}
        self.total_task = 0
        self.completed_task = 0
        self.start_time = time.time()
        self.execution_id = ""

    def set_total_task(self, total):
        self.total_task = total

    def set_execution_id(self, exec_id):
        self.execution_id = exec_id

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, remark="-"):
        with self.lock:
            self.results.append({
                'table': table_name, 
                'status': status,
                'remark': str(remark).replace('\n', ' ')
            })
            self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
        
    def print_summary(self, log_path, output_dir):
        success_count, failed_count, loaded_count, skipped_count = 0, 0, 0, 0
        for r in self.results:
            if r['status'] == 'PASSED': success_count += 1
            elif r['status'] == 'LOADED': loaded_count += 1
            elif r['status'] == 'SKIPPED': skipped_count += 1
            else: failed_count += 1

        elapsed_seconds = int(time.time() - self.start_time)
        h, rem = divmod(elapsed_seconds, 3600)
        m, s = divmod(rem, 60)
        duration_str = "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)

        summary_lines = [
            "",
            "="*80,
            "FINAL SUMMARY REPORT",
            "="*80,
            "Execution ID : {0}".format(self.execution_id),
            "Total Task   : {0}".format(len(self.results)),
            "PASSED       : {0}".format(success_count),
            "LOADED       : {0}".format(loaded_count),
            "SKIPPED      : {0}".format(skipped_count),
            "FAILED       : {0}".format(failed_count),
            "Duration     : {0}".format(duration_str),
            "Log File     : {0}".format(log_path),
            "Output Dir   : {0}".format(output_dir),
            "="*80
        ]
        
        for line in summary_lines:
            self.logger.info(line)
            print(line)

class MonitorThread(threading.Thread):
    def __init__(self, tracker, num_workers, log_path):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.num_workers = num_workers
        self.log_path = log_path
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard()

    def print_dashboard(self):
        comp, total = self.tracker.get_progress()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        lines = []
        lines.append("============================================================")
        lines.append(" RECONCILE COMPARE MONITOR (Python 2.7) ")
        lines.append(" Execution ID: {0}".format(self.tracker.execution_id))
        lines.append("============================================================")
        lines.append(" Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct))
        lines.append(" Elapsed : {0:.0f}s".format(elapsed))
        lines.append("-" * 60)

        workers = sorted(self.tracker.worker_status.keys())
        for w_name in workers:
            status = self.tracker.worker_status.get(w_name, "Initializing...")
            lines.append(" {0} : {1}".format(w_name, status)[:79]) 
        
        lines.append("-" * 60)
        lines.append(" Log File: {0}".format(self.log_path))

        if not self.first_print:
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        sys.stdout.write(output)
        sys.stdout.flush()

def setup_logging(log_dir, log_name="app", timestamp=None):
    try:
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))
        logger = logging.getLogger("CompareJob")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        fh = logging.FileHandler(log_file)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        return logger, log_file
    except Exception as e:
        print("CRITICAL ERROR: Cannot create log directory or file. Permission denied or disk full? Error: {0}".format(e))
        sys.exit(1)

# ==============================================================================
# 2. Configuration & Initialization
# ==============================================================================

class ConfigManager(object):
    def __init__(self, args, logger, execution_id, log_path):
        self.logger = logger
        self.mode = args.mode
        self.args = args
        self.execution_id = execution_id
        self.log_path = log_path
        
        self.succeed_log_gp_path = ''
        self.succeed_log_pq_path = ''
        self.replace_path_from = ''
        self.replace_path_to = ''
        self.metadata_base_dir = ''
        self.hive_status_table = ''
        self.hive_result_table = ''
        self.kinit_cmd = ''
        
        self._load_env_config(args.env)
        self._log_configurations()
        self.execution_list = self._build_queue(args.list, args.table_name)

    def _load_env_config(self, env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, val = [x.strip() for x in line.split('=', 1)]
                        if key == 'succeed_log_gp_path': self.succeed_log_gp_path = val
                        elif key == 'succeed_log_pq_path': self.succeed_log_pq_path = val
                        elif key == 'replace_path_from': self.replace_path_from = val
                        elif key == 'replace_path_to': self.replace_path_to = val
                        elif key == 'metadata_base_dir': self.metadata_base_dir = val
                        elif key == 'conda_activate_cmd': self.conda_activate_cmd = val
                        elif key == 'kinit': self.kinit_cmd = val
                        elif key == 'hive_status_table': self.hive_status_table = val
                        elif key == 'hive_result_table': self.hive_result_table = val
        except Exception as e:
            self.logger.critical("Cannot read env config: {0}".format(e))
            err_msg = "Cannot read env config file: {0} | Error: {1}".format(env_path, e)
            self.logger.critical(err_msg)
            print("\n============================================================")
            print(" [CRITICAL ERROR] Failed to load Environment Configuration")
            print(" File Path : {0}".format(env_path))
            print(" Details   : {0}".format(e))
            print("============================================================\n")
            raise

    def _build_queue(self, list_path, cli_tables):
        exec_list = []
        seen_tables = set()

        def add_task(db_part, sch_part, real_tbl):
            task_key = "{0}.{1}.{2}".format(db_part, sch_part, real_tbl)
            if task_key not in seen_tables:
                seen_tables.add(task_key)
                exec_list.append({'db': db_part, 'schema': sch_part, 'partition': real_tbl})

        if cli_tables:
            for t in cli_tables.split(','):
                try:
                    db_part, tbl_part = t.split('|')
                    sch_part, real_tbl = tbl_part.split('.')
                    
                    db_clean = db_part.strip()
                    sch_clean = sch_part.strip()
                    tbl_clean = real_tbl.strip()
                    
                    if not db_clean or not sch_clean or not tbl_clean:
                        raise ValueError("Missing component")
                        
                    add_task(db_clean, sch_clean, tbl_clean)
                except ValueError:
                    err_msg = "Invalid table_name format: '{0}'. Expected 'db|schema.table_name' with all parts filled.".format(t)
                    print("\n============================================================")
                    print(" [CRITICAL ERROR] " + err_msg)
                    print(" Log file: {0}".format(self.log_path))
                    print("============================================================\n")
                    raise Exception(err_msg)
        elif list_path:
            try:
                self.logger.info("Reading table list from: {0}".format(list_path))
                with open(list_path, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.lower().strip()
                        if not line or line.startswith('#'): continue
                        try:
                            db_part, tbl_part = line.split('|')
                            sch_part, real_tbl = tbl_part.split('.')
                            
                            db_clean = db_part.strip()
                            sch_clean = sch_part.strip()
                            tbl_clean = real_tbl.strip()
                            
                            if not db_clean or not sch_clean or not tbl_clean:
                                raise ValueError("Missing component")
                                
                            add_task(db_clean, sch_clean, tbl_clean)
                        except ValueError:
                            err_msg = "Invalid format at line {0} in {1}: '{2}'. Expected 'db|schema.table_name' with all parts filled.".format(line_num, list_path, line)
                            print("\n============================================================")
                            print(" [CRITICAL ERROR] " + err_msg)
                            print(" Log file: {0}".format(self.log_path))
                            print("============================================================\n")
                            raise Exception(err_msg)
            except Exception as e:
                err_msg = "Cannot read or parse list table file: {0} | Error: {1}".format(list_path, e)
                self.logger.critical(err_msg, exc_info=True)                
                print("\n" + "="*60)
                print(" [CRITICAL ERROR] Failed to open Table List file")
                print(" File Path : {0}".format(list_path))
                print(" Details   : {0}".format(e))
                print("="*60 + "\n")
                raise
                
        self.logger.info("Successfully loaded {0} unique tables into queue.".format(len(exec_list)))
        
        if len(exec_list) == 0:
            err_msg = "Execution list is EMPTY. No tables to process. Aborting."
            self.logger.critical(err_msg)
            print("\n============================================================")
            print(" [CRITICAL ERROR] " + err_msg)
            print(" Please check your input file: {0}".format(list_path or cli_tables))
            print(" Log file: {0}".format(self.log_path))
            print("============================================================\n")
            raise Exception(err_msg)
            
        return exec_list
    
    def _log_configurations(self):
        self.logger.info("--- Configuration Settings ---")
        self.logger.info("Mode                 : {0}".format(self.mode))
        self.logger.info("Concurrency          : {0}".format(self.args.concurrency))
        self.logger.info("Env File             : {0}".format(self.args.env))
        self.logger.info("Table List Input     : {0}".format(self.args.list or self.args.table_name))
        self.logger.info("GP Log Path          : {0}".format(self.succeed_log_gp_path))
        self.logger.info("PQ Log Path          : {0}".format(self.succeed_log_pq_path))
        self.logger.info("Hive Status Table    : {0}".format(self.hive_status_table))
        self.logger.info("Hive Result Table    : {0}".format(self.hive_result_table))
        self.logger.info("Execution ID         : {0}".format(self.execution_id))
        self.logger.info("Log File Path        : {0}".format(self.log_path))
        self.logger.info("------------------------------")

# ==============================================================================
# 3. Core Processing Modules
# ==============================================================================
    
class SucceededLogValidator(object):
    def __init__(self, gp_log_path, pq_log_path, logger):
        self.logger = logger
        self.gp_log_path = gp_log_path
        self.pq_log_path = pq_log_path
        self.cache = {'gp': {}, 'pq': {}}
        self.lock = threading.Lock()

    def _load_cache(self, source, db, schema):
        base_path = self.gp_log_path if source == 'gp' else self.pq_log_path
        cache_key = "{0}_{1}".format(db, schema)
        
        if cache_key not in self.cache[source]:
            self.cache[source][cache_key] = {}
            pattern = os.path.join(base_path, "*", db, schema, "stat_csv", "log_stat_rc_*.csv")
            matched_files = sorted(glob.glob(pattern), reverse=True)

            self.logger.info("LogValidator [{0}]: Found {1} files for {2}.{3}".format(source, len(matched_files), db, schema))
            
            for log_file in matched_files:
                try:
                    with open(log_file, 'r') as f:
                        fields = [
                            "greenplum_tbl", "start_timestamp", "end_timestamp", "duration", 
                            "reconcile_method", "run_status", "error_message", "json_output_path", "remark"
                        ]
                        reader = csv.DictReader(f, fieldnames=fields)
                        for row in reader:
                            tbl = row.get('greenplum_tbl', '').strip().replace('"', '').replace("'", "")
                            if tbl and tbl != 'greenplum_tbl':
                                current_ts = self.cache[source][cache_key].get(tbl, {}).get('end_timestamp', '')
                                new_ts = row.get('end_timestamp', '').strip()
                                
                                if not current_ts or new_ts > current_ts:
                                    row['run_status'] = row.get('run_status', '').strip()
                                    row['json_output_path'] = row.get('json_output_path', '').strip()
                                    row['source_file_path'] = log_file
                                    self.cache[source][cache_key][tbl] = row
                except Exception as e:
                    self.logger.warning("Error reading log file {0}: {1}".format(log_file, e))
                    continue

    def get_json_paths(self, db, schema, partition, mode):
        with self.lock:
            if mode in ['compare', 'load_gp', 'load_both']:
                self._load_cache('gp', db, schema)
            if mode in ['compare', 'load_pq', 'load_both']:
                self._load_cache('pq', db, schema)

        cache_key = "{0}_{1}".format(db, schema)
        
        target_name_1 = partition.strip()
        target_name_2 = "{0}.{1}".format(schema, partition).strip()
        target_name_3 = "{0}.{1}.{2}".format(db, schema, partition).strip()

        def find_row_in_cache(src):
            cache_dict = self.cache[src].get(cache_key, {})
            return cache_dict.get(target_name_1) or cache_dict.get(target_name_2) or cache_dict.get(target_name_3)

        gp_path, pq_path = None, None
        gp_err, pq_err = "", ""

        if mode in ['compare', 'load_gp', 'load_both']:
            row = find_row_in_cache('gp')
            if row:
                self.logger.info("LogValidator [GP]: Latest record for '{0}' has status '{1}' at '{2}' [Source: {3}]".format(
                    partition, row.get('run_status'), row.get('end_timestamp'), row.get('source_file_path')))
                if row.get('run_status') == 'SUCCEEDED':
                    gp_path = row.get('json_output_path', '').strip()
                else:
                    gp_err = "Latest query greenplum status is {0}".format(row.get('run_status'))
            else:
                gp_err = "Table not found in greenplum log"

        if mode in ['compare', 'load_pq', 'load_both']:
            row = find_row_in_cache('pq')
            if row:
                self.logger.info("LogValidator [PQ]: Latest record for '{0}' has status '{1}' at '{2}' [Source: {3}]".format(
                    partition, row.get('run_status'), row.get('end_timestamp'), row.get('source_file_path')))
                if row.get('run_status') == 'SUCCEEDED':
                    pq_path = row.get('json_output_path', '').strip()
                else:
                    pq_err = "Latest query parquet status is {0}".format(row.get('run_status'))
            else:
                pq_err = "Table not found in parquet log"

        return gp_path, pq_path, gp_err, pq_err
    
class JsonHandler(object):
    def __init__(self, replace_from, replace_to, logger):
        self.logger = logger
        self.r_from = replace_from
        self.r_to = replace_to

    def _read_json(self, file_path):
        if not file_path: return None, "Path is None"
        if self.r_from and self.r_to:
            file_path = file_path.replace(self.r_from, self.r_to)
        
        if not os.path.exists(file_path):
            return None, "File not found: {0}".format(file_path)
        try:
            with open(file_path, 'r') as f:
                return json.load(f), None
        except Exception as e:
            return None, "Invalid JSON: {0}".format(e)

    def fetch_and_validate(self, gp_path, pq_path, mode):
        gp_data, pq_data = None, None
        gp_err, pq_err = None, None

        if mode in ['compare', 'load_gp', 'load_both']:
            gp_data, gp_err = self._read_json(gp_path)
        if mode in ['compare', 'load_pq', 'load_both']:
            pq_data, pq_err = self._read_json(pq_path)

        return gp_data, pq_data, gp_err, pq_err
    
class ReconcileMain(object):
    def __init__(self, logger):
        self.logger = logger

    def compare(self, gp_data, pq_data):
        """ Sequential Gating Logic for Comparison """
        res = {
            'gp_count_record': gp_data.get('count', 0), 'pq_count_record': pq_data.get('count', 0),
            'gp_struct': {'SUM_MIN_MAX': 0, 'MIN_MAX': 0, 'MD5_MIN_MAX': 0},
            'pq_struct': {'SUM_MIN_MAX': 0, 'MIN_MAX': 0, 'MD5_MIN_MAX': 0},
            'columns': [],
            'status': 'PASSED',
            'remark': 'Successfully Matched'
        }

        for m in ['SUM_MIN_MAX', 'MIN_MAX', 'MD5_MIN_MAX']:
            res['gp_struct'][m] = len(gp_data.get('methods', {}).get(m, {}))
            res['pq_struct'][m] = len(pq_data.get('methods', {}).get(m, {}))

        # Gate 1: Row Count
        if res['gp_count_record'] != res['pq_count_record']:
            res['status'] = 'FAILED'
            res['remark'] = 'Record count mismatch'
            return res

        # Gate 2: Structure Count
        if res['gp_struct'] != res['pq_struct']:
            res['status'] = 'FAILED'
            res['remark'] = 'No. of methods column count mismatch'
            return res

        # Gate 3: Content Match
        all_methods = set(gp_data.get('methods', {}).keys() + pq_data.get('methods', {}).keys())
        
        has_datatype_mismatch = False
        has_content_mismatch = False

        for method in all_methods:
            gp_cols = gp_data.get('methods', {}).get(method, {})
            pq_cols = pq_data.get('methods', {}).get(method, {})
            all_cols = set(gp_cols.keys() + pq_cols.keys())

            for col in all_cols:
                gp_vals = gp_cols.get(col, {})
                pq_vals = pq_cols.get(col, {})
                
                gp_dtype = gp_vals.get('data_type')
                pq_dtype = pq_vals.get('data_type')
                dtype = gp_dtype or pq_dtype or 'UNKNOWN'
                
                col_res = {
                    'col_nm': col, 
                    'method': method, 
                    'gp_vals': gp_vals, 
                    'pq_vals': pq_vals, 
                    'status': 'PASSED', 
                    'data_type': dtype,
                    'remark': None
                }
                
                col_errors = []
                
                if gp_dtype != pq_dtype:
                    has_datatype_mismatch = True
                    col_errors.append("Data type mismatched: GP={0}, PQ={1}".format(gp_dtype, pq_dtype))
                
                content_diffs = []
                keys_to_check = []
                if method == 'SUM_MIN_MAX':
                    keys_to_check = ['sum', 'min', 'max']
                elif method == 'MIN_MAX':
                    keys_to_check = ['min', 'max']
                elif method == 'MD5_MIN_MAX':
                    keys_to_check = ['min_md5', 'max_md5']
                    
                for k in keys_to_check:
                    if gp_vals.get(k) != pq_vals.get(k):
                        content_diffs.append(k)
                        
                if content_diffs:
                    has_content_mismatch = True
                    col_errors.append("Content mismatched in: {0}".format(", ".join(content_diffs)))
                
                if col_errors:
                    col_res['status'] = 'FAILED'
                    col_res['remark'] = " | ".join(col_errors)
                
                res['columns'].append(col_res)

        if has_datatype_mismatch and has_content_mismatch:
            res['status'] = 'FAILED'
            res['remark'] = 'Some columns data type and content mismatched'
        elif has_datatype_mismatch:
            res['status'] = 'FAILED'
            res['remark'] = 'Some columns data type mismatched'
        elif has_content_mismatch:
            res['status'] = 'FAILED'
            res['remark'] = 'Some columns content mismatched'

        return res
    
class ResultDataHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def format_results(self, db, schema, partition, execution_id, raw_result, mode, gp_path, pq_path, start_ts, error_state=None):
        base_tbl = partition.split('_1_prt_')[0] if '_1_prt_' in partition else partition
        full_table = "{0}.{1}.{2}".format(db, schema, partition)
        
        end_ts = datetime.now()
        
        # Base Header
        h_rec = {
            'table_name': full_table,
            'gp_count_record': None,
            'pq_count_record': None,
            'gp_count_sum_min_max_col': None,
            'pq_count_sum_min_max_col': None,
            'gp_count_min_max_col': None,
            'pq_count_min_max_col': None,
            'gp_count_md5_min_max_col': None,
            'pq_count_md5_min_max_col': None,
            'gp_total_recon_col': None,
            'pq_total_recon_col': None,
            'reconcile_status': 'UNKNOWN', 
            'remark': None,
            'gp_json_file': gp_path or None, 
            'pq_json_file': pq_path or None,
            'start_ts': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'execution_id': execution_id
        }

        d_recs = []

        if error_state:
            if error_state.startswith("SKIPPED:"):
                h_rec['reconcile_status'] = 'SKIPPED'
                h_rec['remark'] = error_state.replace("SKIPPED: ", "")
            else:
                h_rec['reconcile_status'] = 'UPSTREAM-FAILED' if 'UPSTREAM' in error_state else 'MISSING-FILE'
                h_rec['remark'] = error_state
            return h_rec, d_recs

        if mode in ['load_gp', 'load_pq', 'load_both']:
            h_rec['reconcile_status'] = 'LOADED'
            if mode == 'load_gp':
                h_rec['remark'] = 'Load Greenplum JSON Only Mode'
            elif mode == 'load_pq':
                h_rec['remark'] = 'Load Parquet JSON Only Mode'
            else:
                h_rec['remark'] = 'Load Greenplum and Parquet JSON Only Mode'
            # Load dummy raw_result from whichever is available
            gp_struct = raw_result.get('gp_struct', {})
            pq_struct = raw_result.get('pq_struct', {})
            
            h_rec['gp_count_record'] = raw_result.get('gp_count_record')
            h_rec['pq_count_record'] = raw_result.get('pq_count_record')
            h_rec['gp_count_sum_min_max_col'] = gp_struct.get('SUM_MIN_MAX')
            h_rec['pq_count_sum_min_max_col'] = pq_struct.get('SUM_MIN_MAX')
            h_rec['gp_count_min_max_col'] = gp_struct.get('MIN_MAX')
            h_rec['pq_count_min_max_col'] = pq_struct.get('MIN_MAX')
            h_rec['gp_count_md5_min_max_col'] = gp_struct.get('MD5_MIN_MAX')
            h_rec['pq_count_md5_min_max_col'] = pq_struct.get('MD5_MIN_MAX')
            
            h_rec['gp_total_recon_col'] = sum(gp_struct.values()) if gp_struct else None
            h_rec['pq_total_recon_col'] = sum(pq_struct.values()) if pq_struct else None
            
            for col_res in raw_result.get('columns', []):
                d_recs.append(self._build_detail_rec(full_table, col_res, execution_id , 'LOADED'))

        else:
            # Compare Mode
            h_rec['reconcile_status'] = raw_result['status']
            h_rec['remark'] = raw_result['remark']
            
            h_rec['gp_count_record'] = raw_result['gp_count_record']
            h_rec['pq_count_record'] = raw_result['pq_count_record']
            
            gs = raw_result['gp_struct']
            ps = raw_result['pq_struct']
            h_rec['gp_count_sum_min_max_col'], h_rec['pq_count_sum_min_max_col'] = gs['SUM_MIN_MAX'], ps['SUM_MIN_MAX']
            h_rec['gp_count_min_max_col'], h_rec['pq_count_min_max_col'] = gs['MIN_MAX'], ps['MIN_MAX']
            h_rec['gp_count_md5_min_max_col'], h_rec['pq_count_md5_min_max_col'] = gs['MD5_MIN_MAX'], ps['MD5_MIN_MAX']
            h_rec['gp_total_recon_col'], h_rec['pq_total_recon_col'] = sum(gs.values()), sum(ps.values())

            for col_res in raw_result['columns']:
                d_recs.append(self._build_detail_rec(full_table, col_res, execution_id, col_res['status']))

        return h_rec, d_recs
    
    def _build_detail_rec(self, table, col_res, execution_id, status):
        method = col_res['method']
        gp_v, pq_v = col_res['gp_vals'], col_res['pq_vals']
        
        rec = {
            'table_name': table, 'col_nm': col_res['col_nm'], 
            'data_type': col_res.get('data_type', 'UNKNOWN'),
            'method': method,
            'gp_sum': None, 'gp_min': None, 'gp_max': None,
            'pq_sum': None, 'pq_min': None, 'pq_max': None,
            'reconcile_result': status, 'execution_id': execution_id, 
            'remark': col_res.get('remark')
        }

        if method == 'SUM_MIN_MAX':
            rec['gp_sum'], rec['gp_min'], rec['gp_max'] = gp_v.get('sum'), gp_v.get('min'), gp_v.get('max')
            rec['pq_sum'], rec['pq_min'], rec['pq_max'] = pq_v.get('sum'), pq_v.get('min'), pq_v.get('max')
        elif method == 'MIN_MAX':
            rec['gp_min'], rec['gp_max'] = gp_v.get('min'), gp_v.get('max')
            rec['pq_min'], rec['pq_max'] = pq_v.get('min'), pq_v.get('max')
        elif method == 'MD5_MIN_MAX':
            # MD5 stored in min/max columns
            rec['gp_min'], rec['gp_max'] = gp_v.get('min_md5'), gp_v.get('max_md5')
            rec['pq_min'], rec['pq_max'] = pq_v.get('min_md5'), pq_v.get('max_md5')
            
        return rec
    
# ==============================================================================
# 4. Output Writers
# ==============================================================================

class ReportWriter(object):
    def __init__(self, out_dir, global_ts, logger):
        self.logger = logger
        self.header_csv = os.path.join(out_dir, "Compare_Header_Result_{0}.csv".format(global_ts))
        self.detail_csv = os.path.join(out_dir, "Compare_Detail_Result_{0}.csv".format(global_ts))
        self.lock = threading.Lock()
        
        self.h_cols = ['table_name', 'gp_count_record', 'pq_count_record', 'gp_count_sum_min_max_col', 'pq_count_sum_min_max_col',
                       'gp_count_min_max_col', 'pq_count_min_max_col', 'gp_count_md5_min_max_col', 'pq_count_md5_min_max_col',
                       'gp_total_recon_col', 'pq_total_recon_col', 'reconcile_status', 'gp_json_file', 'pq_json_file', 
                       'start_ts', 'end_ts', 'execution_id', 'remark']
        self.d_cols = ['table_name', 'col_nm', 'data_type', 'method', 'gp_sum', 'gp_min', 'gp_max', 'pq_sum', 'pq_min', 'pq_max',
                       'reconcile_result', 'execution_id', 'remark']
                       
        try:
            with open(self.header_csv, 'w') as f:
                csv.DictWriter(f, fieldnames=self.h_cols).writeheader()
            with open(self.detail_csv, 'w') as f:
                csv.DictWriter(f, fieldnames=self.d_cols).writeheader()
        except Exception as e:
            self.logger.critical("Failed to initialize Output CSV files. Path: {0}. Error: {1}".format(out_dir, e))
            raise Exception("ReportWriter Initialization Failed: {0}".format(e))

    def append_results(self, h_rec, d_recs):
        with self.lock:
            try:
                with open(self.header_csv, 'a') as f:
                    csv.DictWriter(f, fieldnames=self.h_cols).writerow(h_rec)
                with open(self.detail_csv, 'a') as f:
                    writer = csv.DictWriter(f, fieldnames=self.d_cols)
                    for rec in d_recs: writer.writerow(rec)
            except Exception as e:
                self.logger.critical("ReportWriter Failed to write CSVs! Error: {0}".format(e), exc_info=True)
                raise
    
class VenvParquetHandler(object):
    def __init__(self, activate_cmd, writer_script_path, local_status, local_result, hdfs_status, hdfs_result, logger):
        self.activate_cmd = activate_cmd
        self.writer_script = writer_script_path
        self.local_status = local_status
        self.local_result = local_result
        self.hdfs_status = hdfs_status
        self.hdfs_result = hdfs_result
        self.logger = logger
        self.lock = threading.Lock()

        try:
            if self.local_status and not os.path.exists(self.local_status):
                os.makedirs(self.local_status)
            if self.local_result and not os.path.exists(self.local_result):
                os.makedirs(self.local_result)
        except Exception as e:
            logger.critical("Cannot create local temp parquet directories. Error: {0}".format(e))
            raise Exception("VenvParquetHandler Initialization Failed")

    def log_results(self, h_rec, d_recs, worker=None):
        if not self.local_status or not self.local_result: 
            return
        
        def _write_log(level, msg, exc_info=False):
            if worker and hasattr(worker, '_log'):
                worker._log(level, msg, exc_info)
            else:
                if level == 'INFO': self.logger.info(msg)
                elif level == 'WARNING': self.logger.warning(msg)
                elif level == 'ERROR': self.logger.error(msg, exc_info=exc_info)

        payload = {'status': h_rec, 'result': d_recs}
        fd, temp_json_path = tempfile.mkstemp(suffix='.json')
        
        with self.lock:
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(payload, f)

                cmd_string = "{0} && python {1} {2} {3} {4}".format(
                    self.activate_cmd, 
                    self.writer_script, 
                    temp_json_path, 
                    self.local_status, 
                    self.local_result
                )

                _write_log('INFO', "Step 6: Calling Anaconda to write Parquet for table: {0}".format(h_rec.get('table_name')))
                output = subprocess.check_output(cmd_string, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT)
                _write_log('INFO', "Step 7: Successfully wrote parquet files via Anaconda for: {0}. Output: {1}".format(h_rec.get('table_name'), output.strip()))

                file_uuid = output.strip().decode('utf-8') 
                target_file = "part_{0}.parquet".format(file_uuid)

                try:
                    hdfs_cmd_status = "hdfs dfs -moveFromLocal {0}/{1} {2}/".format(self.local_status, target_file, self.hdfs_status)
                    status_out = subprocess.check_output(hdfs_cmd_status, shell=True, stderr=subprocess.STDOUT)

                    if d_recs:
                        hdfs_cmd_result = "hdfs dfs -moveFromLocal {0}/{1} {2}/".format(self.local_result, target_file, self.hdfs_result)
                        result_out = subprocess.check_output(hdfs_cmd_result, shell=True, stderr=subprocess.STDOUT)

                    _write_log('INFO', "Step 8: Successfully moved Parquet to HDFS for: {0}".format(h_rec.get('table_name')))
                except subprocess.CalledProcessError as e:
                    actual_error = e.output.decode('utf-8', errors='ignore') if e.output else "No output"
                    _write_log('WARNING', "Failed to move Parquet to HDFS for {0}. HDFS Error: {1}".format(h_rec.get('table_name'), actual_error))
                except Exception as e:
                    _write_log('WARNING', "Failed to move Parquet to HDFS for {0}: {1}".format(h_rec.get('table_name'), e))

            except subprocess.CalledProcessError as e:
                actual_error_msg = e.output.decode('utf-8', errors='ignore') if hasattr(e, 'output') and e.output else "No additional error output."
                _write_log('ERROR', "Anaconda Parquet write failed for {0}. Subprocess Exit Code: {1}\n>> ACTUAL ERROR DETAILS:\n{2}".format(
                    h_rec.get('table_name'), e.returncode, actual_error_msg))
            except Exception as e:
                _write_log('ERROR', "Unexpected error in VenvParquetHandler for {0}: {1}".format(h_rec.get('table_name'), e), exc_info=True)
            finally:
                if os.path.exists(temp_json_path):
                    os.remove(temp_json_path)

# ==============================================================================
# 5. Worker & Orchestration
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, log_validator, json_handler, 
                 reconcile_engine, data_handler, report_writer, hive_handler, 
                 tracker, execution_id, logger):
        threading.Thread.__init__(self)
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.log_validator = log_validator
        self.json_handler = json_handler
        self.reconcile_engine = reconcile_engine
        self.data_handler = data_handler
        self.report_writer = report_writer
        self.hive_handler = hive_handler
        self.tracker = tracker
        self.execution_id = execution_id
        self.logger = logger
        self.daemon = True
        self.log_buffer = []

    def _log(self, level, msg, exc_info=False):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        formatted_msg = "{0} [{1}] {2}".format(ts, self.name, msg)
        self.log_buffer.append({'level': level, 'msg': formatted_msg, 'exc_info': exc_info})

    def _flush_logs(self):
        """Writes all buffered logs for the current table to the main logger."""
        for log in self.log_buffer:
            if log['level'] == 'INFO': self.logger.info(log['msg'])
            elif log['level'] == 'WARNING': self.logger.warning(log['msg'])
            elif log['level'] == 'ERROR': self.logger.error(log['msg'], exc_info=log['exc_info'])
        self.log_buffer = []

    def run(self):
        self.logger.info("{0} started and waiting for tasks.".format(self.name))
        while True:
            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.logger.info("{0} queue is empty. Worker exiting.".format(self.name))
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            db, schema, partition = task['db'], task['schema'], task['partition']
            full_name = "{0}.{1}.{2}".format(db, schema, partition)
            start_datetime = datetime.now()
            self.tracker.update_worker_status(self.name, "[BUSY] {0}".format(partition))

            start_datetime = datetime.now()
            start_ts_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')

            self._log('INFO', "--- Starting processing table: {0} | Start: {1} ---".format(full_name, start_ts_str))

            try:
                raw_res, error_state = None, None
                gp_p, pq_p = None, None
                
                # 1. Check Source Logs
                self._log('INFO', "Step 1: Validating Logs".format(self.name))
                gp_p, pq_p, gp_err, pq_err = self.log_validator.get_json_paths(db, schema, partition, self.config.mode)

                self._log('INFO', "Paths Retrieved -> GP_PATH: {0}, PQ_PATH: {1}".format(gp_p, pq_p))
                self._log('INFO', "Errors Retrieved -> GP_ERR: '{0}', PQ_ERR: '{1}'".format(gp_err, pq_err))

                if self.config.replace_path_from and self.config.replace_path_to:
                    if gp_p:
                        original_gp_path = gp_p
                        gp_p = original_gp_path.replace(self.config.replace_path_from, self.config.replace_path_to)
                        self._log('INFO', "Replace GP json output path from: {0} to {1}".format(original_gp_path, gp_p))
                
                if gp_err or pq_err:
                    err_msgs = []
                    if gp_err: err_msgs.append(gp_err)
                    if pq_err: err_msgs.append(pq_err)
                    error_state = "SKIPPED: {0}".format(" | ".join(err_msgs))
                    self._log('WARNING', "{0} -> {1}".format(full_name, error_state))
                else:
                    # 2. Fetch JSON
                    self._log('INFO', "Step 2: Fetching JSONs (GP: {0}, PQ: {1})".format(gp_p, pq_p))
                    gp_data, pq_data, gp_jerr, pq_jerr = self.json_handler.fetch_and_validate(gp_p, pq_p, self.config.mode)
                    if gp_jerr or pq_jerr:
                        error_state = "MISSING-FILE: {0} {1}".format(gp_jerr or '', pq_jerr or '').strip()
                        self._log('WARNING', "{0} -> {1}".format(full_name, error_state))
                    else:
                        # 3. Execution Mode Logic
                        self._log('INFO', "Step 3: Executing Logic (Mode: {0})".format(self.config.mode))
                        if self.config.mode == 'compare':
                            raw_res = self.reconcile_engine.compare(gp_data, pq_data)
                        elif self.config.mode in ['load_gp', 'load_pq', 'load_both']:
                            raw_res = self._build_load_mock(gp_data, pq_data)

                # 4. Format Result
                self._log('INFO', "Step 4: Formatting Results")
                h_rec, d_recs = self.data_handler.format_results(
                    db, schema, partition, self.execution_id, raw_res, 
                    self.config.mode, gp_p, pq_p, self.job_start_time, error_state
                )

                # 5. Write Outputs
                self._log('INFO', "Step 5: Writing Outputs to CSV and Hive")
                self.report_writer.append_results(h_rec, d_recs)
                if self.hive_handler:
                    self.hive_handler.log_results(h_rec, d_recs, self)

                end_datetime = datetime.now()
                end_ts_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')
                duration = end_datetime - start_datetime
                duration_str = str(duration).split('.')[0]

                # 6. Update Tracker
                self.tracker.add_result(full_name, h_rec['reconcile_status'], h_rec['remark'])
                self._log('INFO', "Completed {0} with Status: {1} | End: {2} | Duration: {3}".format(
                    full_name, h_rec['reconcile_status'], end_ts_str, duration_str))

            except Exception as e:
                self._log('ERROR', "FATAL Error on {0}: {1}".format(full_name, e), exc_info=True)
                self.tracker.add_result(full_name, "FATAL-ERROR", str(e))
            finally:
                self.queue.task_done()
                self._flush_logs()
                
    def _build_load_mock(self, gp_data, pq_data):
        res = {'columns': [], 'gp_count_record': None, 'pq_count_record': None, 'gp_struct': {}, 'pq_struct': {}}
        
        if gp_data:
            res['gp_count_record'] = gp_data.get('count')
            for m in ['SUM_MIN_MAX', 'MIN_MAX', 'MD5_MIN_MAX']:
                res['gp_struct'][m] = len(gp_data.get('methods', {}).get(m, {}))
        
        if pq_data:
            res['pq_count_record'] = pq_data.get('count')
            for m in ['SUM_MIN_MAX', 'MIN_MAX', 'MD5_MIN_MAX']:
                res['pq_struct'][m] = len(pq_data.get('methods', {}).get(m, {}))
                
        all_methods = set()
        if gp_data: all_methods.update(gp_data.get('methods', {}).keys())
        if pq_data: all_methods.update(pq_data.get('methods', {}).keys())
        
        for method in all_methods:
            gp_cols = gp_data.get('methods', {}).get(method, {}) if gp_data else {}
            pq_cols = pq_data.get('methods', {}).get(method, {}) if pq_data else {}
            all_cols = set(gp_cols.keys() + pq_cols.keys())
            
            for col in all_cols:
                gp_v = gp_cols.get(col, {})
                pq_v = pq_cols.get(col, {})
                dtype = gp_v.get('data_type') or pq_v.get('data_type') or 'UNKNOWN'
                col_res = {'col_nm': col, 'method': method, 'status': 'LOADED', 'data_type': dtype}
                col_res['gp_vals'] = gp_cols.get(col, {})
                col_res['pq_vals'] = pq_cols.get(col, {})
                res['columns'].append(col_res)
                
        return res

class ReconcileJob(object):
    def __init__(self, args, logger, log_path, main_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.global_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.job_start_time = datetime.now()
        self.out_dir = os.path.join(main_path, 'output', datetime.now().strftime("%Y%m%d"))
        try:
            if not os.path.exists(self.out_dir): os.makedirs(self.out_dir)
        except Exception as e:
            logger.critical("Cannot create output directory: {0}. Error: {1}".format(self.out_dir, e))
            raise Exception("Output Directory Creation Failed")

        short_uuid = str(uuid.uuid4()).split('-')[0]
        self.execution_id = "JOB_{0}_{1}".format(self.global_ts, short_uuid)

        self.config = ConfigManager(args, logger, self.execution_id, self.log_path)
        
        if hasattr(self.config, 'kinit_cmd') and self.config.kinit_cmd:
            self._authenticate_kerberos()

        self.tracker = ProcessTracker(logger)
        self.tracker.set_execution_id(self.execution_id)
        self.tracker.set_total_task(len(self.config.execution_list))        

        # Init Modules
        self.log_validator = SucceededLogValidator(self.config.succeed_log_gp_path, self.config.succeed_log_pq_path, logger)
        self.json_handler = JsonHandler(self.config.replace_path_from, self.config.replace_path_to, logger)
        self.reconcile_engine = ReconcileMain(logger)
        self.data_handler = ResultDataHandler(logger)
        self.report_writer = ReportWriter(self.out_dir, self.global_ts, logger)

        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        writer_script = os.path.join(current_script_dir, "parquet_writer.py")
        local_temp_status = os.path.join(self.out_dir, "temp_parquet", "status")
        local_temp_result = os.path.join(self.out_dir, "temp_parquet", "result")

        self.hive_handler = VenvParquetHandler(
            self.config.conda_activate_cmd,
            writer_script,
            local_temp_status,
            local_temp_result,
            self.config.hive_status_table,
            self.config.hive_result_table,
            logger
        )

        self.job_queue = Queue.Queue()
        for task in self.config.execution_list: self.job_queue.put(task)
    
    def _authenticate_kerberos(self):
        max_retries = 2
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info("Executing Kerberos authentication (Attempt {0}/{1}): {2}".format(attempt, max_retries, self.config.kinit_cmd))
                subprocess.check_output(self.config.kinit_cmd, shell=True, stderr=subprocess.STDOUT)
                self.logger.info("Kerberos authentication successful.")
                return
            except subprocess.CalledProcessError as e:
                error_msg = e.output.decode('utf-8', errors='ignore') if e.output else str(e)
                if attempt < max_retries:
                    self.logger.warning("Kerberos authentication failed on attempt {0}. Retrying... Error: {1}".format(attempt, error_msg))
                    time.sleep(1)
                else:
                    self.logger.critical("Kerberos authentication FAILED after {0} attempts. HDFS commands will fail. \n>> ACTUAL ERROR: {1}".format(max_retries, error_msg))
                    raise Exception("Kerberos Authentication Failed: {0}".format(error_msg))


    def run(self):
        num_workers = self.args.concurrency
        workers = []
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.log_validator, self.json_handler, 
                       self.reconcile_engine, self.data_handler, self.report_writer, 
                       self.hive_handler, self.tracker, self.execution_id, self.logger)
            workers.append(w)
            w.job_start_time = self.job_start_time
            w.start()

        monitor = MonitorThread(self.tracker, num_workers, self.log_path)
        monitor.start()

        try:
            self.job_queue.join()
            for w in workers: w.join()
        except KeyboardInterrupt:
            self.logger.warning("Job Interrupted by User!")
        finally:
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.log_path, self.out_dir)

# ==============================================================================
# Main Entry Point
# ==============================================================================
if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--env', default='env_config.txt')
    parser.add_argument('--concurrency', default=4, type=int)
    parser.add_argument('--mode', choices=['compare', 'load_gp', 'load_pq', 'load_both'], default='compare')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list', help='Name of list of tables file')
    group.add_argument('--table_name', help='Specific tables (DB|Schema.Partition)')
    
    args = parser.parse_args()

    if not args.list and not args.table_name:
        args.list = 'list_table.txt'

    args.env = os.path.join(main_path, 'config', os.path.basename(args.env))
    if args.list:
        args.list = os.path.join(main_path, 'config', os.path.basename(args.list))

    global_date = datetime.now().strftime("%Y%m%d")
    global_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(main_path, 'log', global_date)

    logger, log_path = setup_logging(log_dir, 'reconcile_compare', global_ts)

    try:
        job = ReconcileJob(args, logger, log_path, main_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted: {0}".format(e), exc_info=True)
        print("\n" + "="*60)
        print(" [JOB ABORTED] A critical error occurred!")
        print(" Details  : {0}".format(e))
        print(" Log File : {0}".format(log_path))
        print("="*60 + "\n")
        sys.exit(1)