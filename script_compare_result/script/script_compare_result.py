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
from datetime import datetime

from pyspark.sql import SparkSession

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

    def set_total_task(self, total):
        self.total_task = total

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
        self.logger.info("="*100)
        self.logger.info("RECONCILE COMPARE SUMMARY")
        self.logger.info("="*100)

        success_count, failed_count, loaded_count = 0, 0, 0

        for r in self.results:
            if r['status'] == 'PASSED': success_count += 1
            elif r['status'] == 'LOADED': loaded_count += 1
            else: failed_count += 1
        
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total Task : {0}".format(len(self.results)))
        print("PASSED     : {0}".format(success_count))
        print("LOADED     : {0}".format(loaded_count))
        print("FAILED     : {0}".format(failed_count))
        print("Log File   : {0}".format(log_path))
        print("Output Dir : {0}".format(output_dir))
        print("="*80)

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
        lines.append(" RECONCILE COMPARE MONITOR (Python 2.7 / PySpark) ")
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
    if not os.path.exists(log_dir): os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))
    logger = logging.getLogger("CompareJob")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fh = logging.FileHandler(log_file)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(fh)
    return logger, log_file
# ==============================================================================
# 2. Configuration & Initialization
# ==============================================================================

class ConfigManager(object):
    def __init__(self, args, logger):
        self.logger = logger
        self.mode = args.mode
        
        self.succeed_log_gp_path = ''
        self.succeed_log_pq_path = ''
        self.replace_path_from = ''
        self.replace_path_to = ''
        self.metadata_base_dir = ''
        self.hive_header_table = ''
        self.hive_detail_table = ''
        
        self._load_env_config(args.env)
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
                        elif key == 'hive_header_table': self.hive_header_table = val
                        elif key == 'hive_detail_table': self.hive_detail_table = val
        except Exception as e:
            self.logger.critical("Cannot read env config: {0}".format(e))
            raise

    def _build_queue(self, list_path, cli_tables):
        exec_list = []
        if cli_tables:
            for t in cli_tables.split(','):
                db_part, tbl_part = t.split('|')
                sch_part, real_tbl = tbl_part.split('.')
                exec_list.append({'db': db_part.strip(), 'schema': sch_part.strip(), 'partition': real_tbl.strip()})
        else:
            try:
                self.logger.info("Reading table list from: {0}".format(list_path))
                with open(list_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): continue
                        db_part, tbl_part = line.split('|')
                        sch_part, real_tbl = tbl_part.split('.')
                        exec_list.append({'db': db_part.strip(), 'schema': sch_part.strip(), 'partition': real_tbl.strip()})
            except Exception as e:
                self.logger.critical("Cannot read list table: {0}".format(e), exc_info=True)
                raise
        self.logger.info("Successfully loaded {0} tables into queue.".format(len(exec_list)))
        if len(exec_list) == 0:
            self.logger.warning("WARNING: Execution list is EMPTY. Workers will exit immediately.")
        return exec_list

# ==============================================================================
# 3. Core Processing Modules
# ==============================================================================

class MetadataFetcher(object):
    def __init__(self, base_dir, logger):
        self.base_dir = base_dir
        self.logger = logger

    def fetch_data_types(self, db_name, table_name):
        if not self.base_dir or not os.path.exists(self.base_dir): return {}
        target_dir = os.path.join(self.base_dir, db_name)
        if not os.path.exists(target_dir): return {}
            
        matches = [os.path.join(r, f) for r, _, fs in os.walk(target_dir) for f in fs if f.endswith("_data_type.txt") and table_name in f]
        latest_file = sorted(matches)[-1] if matches else None
        
        type_map = {}
        if latest_file:
            try:
                with open(latest_file, 'r') as f:
                    reader = csv.DictReader(f, delimiter='|')
                    for row in reader:
                        col = row.get('gp_column_nm', '').strip().lower()
                        dt = row.get('gp_datatype', '').strip()
                        if col and dt: type_map[col] = dt
            except Exception:
                pass
        return type_map
    
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

            self.logger.info("LogValidator [{0}]: Found {1} files for {2}.{3} using pattern: {4}".format(source, len(matched_files), db, schema, pattern))
            
            for log_file in matched_files:
                try:
                    with open(log_file, 'r') as f:
                        fields = [
                            "greenplum_tbl", "start_timestamp", "end_timestamp", "duration", 
                            "reconcile_method", "run_status", "error_message", "json_output_path", "remark"
                        ]
                        reader = csv.DictReader(f, fieldnames=fields)
                        for row in reader:
                            tbl = row.get('greenplum_tbl', '')
                            if tbl and tbl != 'greenplum_tbl':
                                current_ts = self.cache[source][cache_key].get(tbl, {}).get('end_timestamp', '')
                                new_ts = row.get('end_timestamp', '')
                                if not current_ts or new_ts > current_ts:
                                    self.cache[source][cache_key][tbl] = row
                except Exception:
                    self.logger.warning("Error reading log file {0}: {1}".format(log_file, e))
                    continue

    def get_json_paths(self, db, schema, partition, mode):
        with self.lock:
            if mode in ['compare', 'load_gp', 'load_both']:
                self._load_cache('gp', db, schema)
            if mode in ['compare', 'load_pq', 'load_both']:
                self._load_cache('pq', db, schema)

        cache_key = "{0}_{1}".format(db, schema)
        target_name = "{0}.{1}".format(schema, partition)
        
        gp_path, pq_path = None, None
        gp_err, pq_err = "", ""

        if mode in ['compare', 'load_gp', 'load_both']:
            row = self.cache['gp'].get(cache_key, {}).get(partition) or self.cache['gp'].get(cache_key, {}).get(target_name)
            if row and row.get('run_status') == 'SUCCEEDED':
                gp_path = row.get('json_output_path')
            else:
                gp_err = "Latest query greenplum status is not SUCCEEDED"

        if mode in ['compare', 'load_pq', 'load_both']:
            row = self.cache['pq'].get(cache_key, {}).get(partition) or self.cache['pq'].get(cache_key, {}).get(target_name)
            if row and row.get('run_status') == 'SUCCEEDED':
                pq_path = row.get('json_output_path')
            else:
                pq_err = "Latest query parquet status is not SUCCEEDED"

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
            'gp_count_tbl': gp_data.get('count', 0), 'pq_count_tbl': pq_data.get('count', 0),
            'gp_struct': {'SUM_MIN_MAX': 0, 'MIN_MAX': 0, 'MD5_MIN_MAX': 0},
            'pq_struct': {'SUM_MIN_MAX': 0, 'MIN_MAX': 0, 'MD5_MIN_MAX': 0},
            'columns': [],
            'status': 'PASSED',
            'remark': 'Successfully Matched'
        }

        # Count Structures
        for m in ['SUM_MIN_MAX', 'MIN_MAX', 'MD5_MIN_MAX']:
            res['gp_struct'][m] = len(gp_data.get('methods', {}).get(m, {}))
            res['pq_struct'][m] = len(pq_data.get('methods', {}).get(m, {}))

        # Gate 1: Row Count
        if res['gp_count_tbl'] != res['pq_count_tbl']:
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
        content_failed = False

        for method in all_methods:
            gp_cols = gp_data.get('methods', {}).get(method, {})
            pq_cols = pq_data.get('methods', {}).get(method, {})
            all_cols = set(gp_cols.keys() + pq_cols.keys())

            for col in all_cols:
                gp_vals = gp_cols.get(col, {})
                pq_vals = pq_cols.get(col, {})

                col_res = {'col_nm': col, 'method': method, 'gp_vals': gp_vals, 'pq_vals': pq_vals, 'status': 'MATCHED'}
                
                # Exact match check
                if gp_vals != pq_vals:
                    col_res['status'] = 'MISMATCHED'
                    content_failed = True
                
                res['columns'].append(col_res)

        if content_failed and res['status'] == 'PASSED':
            res['status'] = 'FAILED'
            res['remark'] = 'Some columns data mismatched'

        return res
    
class ResultDataHandler(object):
    def __init__(self, meta_fetcher, logger):
        self.logger = logger
        self.meta_fetcher = meta_fetcher

    def format_results(self, db, schema, partition, execution_id, raw_result, mode, gp_path, pq_path, start_ts, error_state=None):
        base_tbl = partition.split('_1_prt_')[0] if '_1_prt_' in partition else partition
        type_map = self.meta_fetcher.fetch_data_types(db, base_tbl) or {}
        full_table = "{0}.{1}.{2}".format(db, schema, partition)
        
        end_ts = datetime.now()
        
        # Base Header
        h_rec = {
            'Table': full_table, 'gp_count_tbl': None, 'parq_count_tbl': None,
            'gp_count_sum_min_max_col': None, 'pq_count_sum_min_max_col': None,
            'gp_count_min_max_col': None, 'pq_count_min_max_col': None,
            'gp_count_md5_min_max_col': None, 'pq_count_md5_min_max_col': None,
            'gp_total_recon_col': None, 'pq_total_recon_col': None,
            'reconcile_status': 'UNKNOWN', 'remark': '',
            'gp_json_file': gp_path or '', 'parquet_json_file': pq_path or '',
            'start_ts': start_ts.strftime('%Y-%m-%d %H:%M:%S'), 'end_ts': end_ts.strftime('%Y-%m-%d %H:%M:%S'),
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
            h_rec['remark'] = 'Load JSON Only Mode'
            # Load dummy raw_result from whichever is available
            gp_struct = raw_result.get('gp_struct', {})
            pq_struct = raw_result.get('pq_struct', {})
            
            h_rec['gp_count_tbl'] = raw_result.get('gp_count_tbl')
            h_rec['parq_count_tbl'] = raw_result.get('pq_count_tbl')
            h_rec['gp_count_sum_min_max_col'] = gp_struct.get('SUM_MIN_MAX')
            h_rec['pq_count_sum_min_max_col'] = pq_struct.get('SUM_MIN_MAX')
            h_rec['gp_count_min_max_col'] = gp_struct.get('MIN_MAX')
            h_rec['pq_count_min_max_col'] = pq_struct.get('MIN_MAX')
            h_rec['gp_count_md5_min_max_col'] = gp_struct.get('MD5_MIN_MAX')
            h_rec['pq_count_md5_min_max_col'] = pq_struct.get('MD5_MIN_MAX')
            
            h_rec['gp_total_recon_col'] = sum(gp_struct.values()) if gp_struct else None
            h_rec['pq_total_recon_col'] = sum(pq_struct.values()) if pq_struct else None
            
            for col_res in raw_result.get('columns', []):
                d_recs.append(self._build_detail_rec(full_table, col_res, type_map, execution_id, gp_path, pq_path, h_rec['start_ts'], h_rec['end_ts'], 'LOADED'))

        else:
            # Compare Mode
            h_rec['reconcile_status'] = raw_result['status']
            h_rec['remark'] = raw_result['remark']
            
            h_rec['gp_count_tbl'] = raw_result['gp_count_tbl']
            h_rec['parq_count_tbl'] = raw_result['pq_count_tbl']
            
            gs = raw_result['gp_struct']
            ps = raw_result['pq_struct']
            h_rec['gp_count_sum_min_max_col'], h_rec['pq_count_sum_min_max_col'] = gs['SUM_MIN_MAX'], ps['SUM_MIN_MAX']
            h_rec['gp_count_min_max_col'], h_rec['pq_count_min_max_col'] = gs['MIN_MAX'], ps['MIN_MAX']
            h_rec['gp_count_md5_min_max_col'], h_rec['pq_count_md5_min_max_col'] = gs['MD5_MIN_MAX'], ps['MD5_MIN_MAX']
            h_rec['gp_total_recon_col'], h_rec['pq_total_recon_col'] = sum(gs.values()), sum(ps.values())

            for col_res in raw_result['columns']:
                d_recs.append(self._build_detail_rec(full_table, col_res, type_map, execution_id, gp_path, pq_path, h_rec['start_ts'], h_rec['end_ts'], col_res['status']))

        return h_rec, d_recs
    
    def _build_detail_rec(self, table, col_res, type_map, exec_id, gp_path, pq_path, start_ts, end_ts, status):
        method = col_res['method']
        gp_v, pq_v = col_res['gp_vals'], col_res['pq_vals']
        
        rec = {
            'table': table, 'col_nm': col_res['col_nm'], 
            'data_type': type_map.get(col_res['col_nm'].lower(), 'UNKNOWN'),
            'method': method,
            'gp_sum': None, 'gp_min': None, 'gp_max': None,
            'pq_sum': None, 'pq_min': None, 'pq_max': None,
            'reconcile_result': status, 'remark': '',
            'gp_json_file': gp_path or '', 'parquet_json_file': pq_path or '',
            'start_ts': start_ts, 'end_ts': end_ts, 'execution_id': exec_id
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
        
        self.h_cols = ['Table', 'gp_count_tbl', 'parq_count_tbl', 'gp_count_sum_min_max_col', 'pq_count_sum_min_max_col',
                       'gp_count_min_max_col', 'pq_count_min_max_col', 'gp_count_md5_min_max_col', 'pq_count_md5_min_max_col',
                       'gp_total_recon_col', 'pq_total_recon_col', 'reconcile_status', 'gp_json_file', 'parquet_json_file', 
                       'start_ts', 'end_ts', 'execution_id', 'remark']
        self.d_cols = ['table', 'col_nm', 'data_type', 'method', 'gp_sum', 'gp_min', 'gp_max', 'pq_sum', 'pq_min', 'pq_max',
                       'reconcile_result', 'gp_json_file', 'parquet_json_file', 'start_ts', 'end_ts', 'execution_id', 'remark']
                       
        with open(self.header_csv, 'w') as f:
            csv.DictWriter(f, fieldnames=self.h_cols).writeheader()
        with open(self.detail_csv, 'w') as f:
            csv.DictWriter(f, fieldnames=self.d_cols).writeheader()

    def append_results(self, h_rec, d_recs):
        with self.lock:
            with open(self.header_csv, 'a') as f:
                csv.DictWriter(f, fieldnames=self.h_cols).writerow(h_rec)
            with open(self.detail_csv, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=self.d_cols)
                for rec in d_recs: writer.writerow(rec)
    
class HiveHandler(object):
    def __init__(self, spark_session, header_tbl, detail_tbl, logger):
        self.spark = spark_session
        self.logger = logger
        self.header_tbl = header_tbl
        self.detail_tbl = detail_tbl
        self.lock = threading.Lock()

    def _safe_str(self, val):
        if val is None: return "NULL"
        return "'{0}'".format(str(val).replace("'", "\\'"))

    def log_results(self, h_rec, d_recs):
        if not self.header_tbl or not self.detail_tbl: return

        h_vals = ", ".join([self._safe_str(h_rec.get(k)) for k in [
            'Table', 'gp_count_tbl', 'parq_count_tbl', 'gp_count_sum_min_max_col', 'pq_count_sum_min_max_col',
            'gp_count_min_max_col', 'pq_count_min_max_col', 'gp_count_md5_min_max_col', 'pq_count_md5_min_max_col',
            'gp_total_recon_col', 'pq_total_recon_col', 'reconcile_status', 'gp_json_file', 'parquet_json_file', 
            'start_ts', 'end_ts', 'execution_id', 'remark']])
        
        h_sql = "INSERT INTO TABLE {0} VALUES ({1})".format(self.header_tbl, h_vals)
        
        with self.lock:
            try:
                self.logger.info("Executing Hive INSERT for table: {0}".format(h_rec.get('Table')))
                self.spark.sql(h_sql)
                if d_recs:
                    d_vals_list = []
                    for r in d_recs:
                        vals = ", ".join([self._safe_str(r.get(k)) for k in [
                            'table', 'col_nm', 'data_type', 'method', 'gp_sum', 'gp_min', 'gp_max', 'pq_sum', 'pq_min', 'pq_max',
                            'reconcile_result', 'gp_json_file', 'parquet_json_file', 'start_ts', 'end_ts', 'execution_id', 'remark']])
                        d_vals_list.append("({0})".format(vals))
                    
                    d_sql = "INSERT INTO TABLE {0} VALUES {1}".format(self.detail_tbl, ", ".join(d_vals_list))
                    self.spark.sql(d_sql)
                
                self.logger.info("Successfully inserted {0} into Hive.".format(h_rec.get('Table'))) # ADDED
            except Exception as e:
                self.logger.error("Hive Insert Failed for {0}: {1}".format(h_rec.get('Table'), e), exc_info=True)

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

            self.logger.info("[{0}] --- Starting processing table: {1} ---".format(self.name, full_name))

            try:
                raw_res, error_state = None, None
                gp_p, pq_p = None, None
                
                # 1. Check Source Logs
                self.logger.info("[{0}] Step 1: Validating Logs".format(self.name))
                gp_p, pq_p, gp_err, pq_err = self.log_validator.get_json_paths(db, schema, partition, self.config.mode)

                if self.config.replace_path_from and self.config.replace_path_to:
                    original_path = gp_p
                    gp_p = original_path.replace(self.config.replace_path_from, self.config.replace_path_to)
                    self.logger.info("Replace json output path from: {0} to {1}".format(original_path, gp_p))
                
                if gp_err or pq_err:
                    err_msgs = []
                    if gp_err: err_msgs.append(gp_err)
                    if pq_err: err_msgs.append(pq_err)
                    error_state = "SKIPPED: {0}".format(" | ".join(err_msgs))
                    self.logger.warning("[{0}] {1} -> {2}".format(self.name, full_name, error_state))
                else:
                    # 2. Fetch JSON
                    self.logger.info("[{0}] Step 2: Fetching JSONs (GP: {1}, PQ: {2})".format(self.name, gp_p, pq_p))
                    gp_data, pq_data, gp_jerr, pq_jerr = self.json_handler.fetch_and_validate(gp_p, pq_p, self.config.mode)
                    if gp_jerr or pq_jerr:
                        error_state = "MISSING-FILE: {0} {1}".format(gp_jerr or '', pq_jerr or '').strip()
                        self.logger.warning("[{0}] {1} -> {2}".format(self.name, full_name, error_state))
                    else:
                        # 3. Execution Mode Logic
                        self.logger.info("[{0}] Step 3: Executing Logic (Mode: {1})".format(self.name, self.config.mode))
                        if self.config.mode == 'compare':
                            raw_res = self.reconcile_engine.compare(gp_data, pq_data)
                        elif self.config.mode in ['load_gp', 'load_pq', 'load_both']:
                            raw_res = self._build_load_mock(gp_data, pq_data)

                # 4. Format Result
                self.logger.info("[{0}] Step 4: Formatting Results".format(self.name))
                h_rec, d_recs = self.data_handler.format_results(db, schema, partition, self.execution_id, raw_res, self.config.mode, gp_p, pq_p, start_datetime, error_state)

                # 5. Write Outputs
                self.logger.info("[{0}] Step 5: Writing Outputs to CSV and Hive".format(self.name))
                self.report_writer.append_results(h_rec, d_recs)
                if self.hive_handler:
                    self.hive_handler.log_results(h_rec, d_recs)

                # 6. Update Tracker
                self.tracker.add_result(full_name, h_rec['reconcile_status'], h_rec['remark'])
                self.logger.info("[{0}] Completed {1} with Status: {2}".format(self.name, full_name, h_rec['reconcile_status']))

            except Exception as e:
                self.logger.error("[{0}] FATAL Error on {1}: {2}".format(self.name, full_name, e), exc_info=True)
                self.tracker.add_result(full_name, "FATAL-ERROR", str(e))
            finally:
                self.queue.task_done()
                
    def _build_load_mock(self, gp_data, pq_data):
        res = {'columns': [], 'gp_count_tbl': 0, 'pq_count_tbl': 0, 'gp_struct': {}, 'pq_struct': {}}
        
        if gp_data:
            res['gp_count_tbl'] = gp_data.get('count')
            for m in ['SUM_MIN_MAX', 'MIN_MAX', 'MD5_MIN_MAX']:
                res['gp_struct'][m] = len(gp_data.get('methods', {}).get(m, {}))
        
        if pq_data:
            res['pq_count_tbl'] = pq_data.get('count')
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
                col_res = {'col_nm': col, 'method': method, 'status': 'LOADED'}
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
        self.out_dir = os.path.join(main_path, 'output', datetime.now().strftime("%Y%m%d"))
        if not os.path.exists(self.out_dir): os.makedirs(self.out_dir)

        self.config = ConfigManager(args, logger)
        self.tracker = ProcessTracker(logger)
        self.tracker.set_total_task(len(self.config.execution_list))
        
        self.logger.info("Initializing SparkSession...")
        self.spark = SparkSession.builder.appName("script_compare_result") \
            .config("spark.scheduler.mode", "FAIR").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.execution_id = self.spark.sparkContext.applicationId

        # Init Modules
        self.log_validator = SucceededLogValidator(self.config.succeed_log_gp_path, self.config.succeed_log_pq_path, logger)
        self.json_handler = JsonHandler(self.config.replace_path_from, self.config.replace_path_to, logger)
        self.reconcile_engine = ReconcileMain(logger)
        meta_fetcher = MetadataFetcher(self.config.metadata_base_dir, logger)
        self.data_handler = ResultDataHandler(meta_fetcher, logger)
        self.report_writer = ReportWriter(self.out_dir, self.global_ts, logger)
        self.hive_handler = HiveHandler(self.spark, self.config.hive_header_table, self.config.hive_detail_table, logger)

        self.job_queue = Queue.Queue()
        for task in self.config.execution_list: self.job_queue.put(task)

    def run(self):
        num_workers = self.args.concurrency
        workers = []
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.log_validator, self.json_handler, 
                       self.reconcile_engine, self.data_handler, self.report_writer, 
                       self.hive_handler, self.tracker, self.execution_id, self.logger)
            workers.append(w)
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
            self.spark.stop()
            self.tracker.print_summary(self.log_path, self.out_dir)

# ==============================================================================
# Main Entry Point
# ==============================================================================
if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser()
    parser.add_argument('--env', default='env_config.txt')
    parser.add_argument('--list', default='list_table.txt')
    parser.add_argument('--table_name', help='Specific tables (DB|Schema.Partition)')
    parser.add_argument('--concurrency', default=4, type=int)
    parser.add_argument('--mode', choices=['compare', 'load_gp', 'load_pq', 'load_both'], default='compare')
    args = parser.parse_args()

    args.env = os.path.join(main_path, 'config', os.path.basename(args.env))
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
        sys.exit(1)