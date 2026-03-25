#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-- Input File Pattern --#
# <database>|<schema>.<table>

#-- Stat File Columns --#
# 1) greenplum_tbl
# 2) start_timestamp
# 3) end_timestamp
# 4) duration
# 5) reconcile_method
# 6) run_status
# 7) error_message
# 8) json_output_path
# 9) remark


import sys
import os
import csv
import logging
import subprocess
import shutil
import time
import argparse
import threading
import Queue
import re
import json
import glob
from datetime import datetime
import uuid

# ==============================================================================
# 1. Utilities: Logger & ProcessTracker
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

    def set_total_tasks(self, total):
        self.total_task = total

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, message="-"):
        with self.lock:
            self.results.append({
                'table': table_name,
                'status': status,
                'message': str(message).replace('\n', ' ')
            })
            self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
    
    def log_step(self, step_name, duration):
        self.logger.info("Step: {0} | Duration: {1:.2f}s".format(step_name, duration))

    def print_summary(self, log_path):
        self.logger.info("="*80)
        self.logger.info("TABLE EXECUTION SUMMARY")
        self.logger.info("="*80)

        success_count = 0
        skipped_count = 0
        failed_count = 0

        if not self.results:
            self.logger.info("No tables processed.")
        else:
            h_table = "Table Name"
            h_status = "Status"
            h_msg = "Error / Remark"

            max_w_table = len(h_table)
            max_w_status = len(h_status)

            for r in self.results:
                if len(r['table']) > max_w_table: max_w_table = len(r['table'])
                if len(r['status']) > max_w_status: max_w_status = len(r['status'])

                if r['status'] == 'SUCCEEDED': success_count += 1
                elif r['status'] == 'SKIPPED': skipped_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
            
            w_table = max_w_table + 2
            w_status = max_w_status + 2

            row_fmt = "{0:<{wt}} | {1:<{ws}} | {2}"

            header_line = row_fmt.format(h_table, h_status, h_msg, wt=w_table, ws=w_status)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 50: sep_line = "-" * 50

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            for r in self.results:
                self.logger.info(row_fmt.format(r['table'], r['status'], r['message'], wt=w_table, ws=w_status))
            
            self.logger.info(sep_line)
            self.logger.info("Total: {0} | Success: {1} | Skipped: {2} | Failed: {3}".format(
                len(self.results), success_count, skipped_count, failed_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Print Summary to Console (Last view)
        print("\n" + "="*80)
        print(" FINAL SUMMARY REPORT")
        print("="*80)
        print(" Total: {0}".format(len(self.results)))
        print(" Success: {0}".format(success_count))
        print(" Skipped: {0}".format(skipped_count))
        print(" Failed:  {0}".format(failed_count))
        print(" Log File: {0}".format(log_path))
        print("="*80)

class LogParser(object):
    def __init__(self, succeed_base_path, logger):
        self.succeed_base_path = succeed_base_path
        self.logger = logger
        self.cache = {} 
        self.lock = threading.Lock()

    def get_latest_succeed_info(self, db, schema, table):
        if self.succeed_base_path is None:
            self.logger.warning("[LogParser] succeed_path is not defined in env_config.txt")
            return None, "succeed_path is not configured"
        cache_key = "{0}_{1}".format(db, schema)
        
        with self.lock:
            if cache_key not in self.cache:
                self.logger.info("[LogParser] Building memory cache for DB: {0}, Schema: {1} ...".format(db, schema))
                self.cache[cache_key] = {}
                
                search_pattern = os.path.join(self.succeed_base_path, db, "*", "offloadgp_stat.{0}.csv".format(schema))
                self.logger.info("[LogParser] Searching for log using pattern: {0}".format(search_pattern))
                matched_files = sorted(glob.glob(search_pattern), reverse=True)
                
                if not matched_files:
                    self.logger.warning("[LogParser] Log files not found for pattern: {0}".format(search_pattern))
                else:
                    expected_fields = [
                        "Run_ID", "Greenplum_Tbl", "Hive_Tbl", 
                        "Start_Timestamp_Script", "End_Timestamp_Script", "Duration_Script", 
                        "Start_Timestamp_Spark", "End_Timestamp_Spark", "Duration_Spark", 
                        "Run_Status", "Error_Message", "Source_Count", 
                        "Target_Count", "Size", "Avg_Row_Len", 
                        "File_Path", "Remark"
                    ]
                    for log_file in matched_files:
                        self.logger.info("[LogParser] Scanning log file into cache: {0}".format(log_file))
                        try:
                            with open(log_file, 'r') as f:
                                reader = csv.DictReader(f, fieldnames=expected_fields)
                                for row in reader:
                                    gp_tbl = row.get('Greenplum_Tbl', '')
                                    new_end_ts = row.get('End_Timestamp_Script', '')

                                    if gp_tbl == 'Greenplum_Tbl':
                                        continue
                                    
                                    if gp_tbl:
                                        prev = self.cache[cache_key].get(gp_tbl)
                                        if prev is None or new_end_ts > prev.get('End_Timestamp_Script', ''):
                                            self.cache[cache_key][gp_tbl] = row
                                            
                        except Exception as e:
                            self.logger.warning("[LogParser] Error parsing log file {0}: {1}".format(log_file, e))
                self.logger.info("[LogParser] Cache build completed. Total {0} tables cached.".format(len(self.cache[cache_key])))

        # Check partition/table name against cache
        target_table_with_schema = "{0}.{1}".format(schema, table)
        latest_row = self.cache[cache_key].get(table) or self.cache[cache_key].get(target_table_with_schema)
        
        if latest_row:
            latest_status = latest_row.get('Run_Status', '').upper()
            if latest_status == 'SUCCEEDED':
                self.logger.info("[LogParser] Found latest Export status = SUCCEEDED for {0} from Cache.".format(table))
                return latest_status, "Found latest Export Status = SUCCEEDED"
            else:
                self.logger.warning("[LogParser] Latest status of table: {0} is not SUCCEEDED (status = {1})".format(table, latest_status))
                return None, "Latest Export status is not SUCCEEDED"
        else:
            self.logger.warning("[LogParser] Not found any Export Status of table: {0}".format(table))
            return None, "Not found any Export Status"
    
def setup_logging(log_dir, log_name="app", date_folder=None, timestamp=None):
    if date_folder:
        log_dir = os.path.join(log_dir, date_folder)

    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Using current directory. Error: {1}".format(log_dir, e))
            log_dir = '.'

    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))

    logger = logging.getLogger("GreenplumBatch")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger, log_file

def peek_env_config(env_path, key_to_find):
    value = None
    try:
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # skip Header
                    if not line or line.startswith('#'):
                        continue
                    
                    if '=' in line:
                        k, v = line.split('=', 1)
                        if k.strip() == key_to_find:
                            value = v.strip()
    except Exception:
        pass
    return value

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class Config(object):
    def __init__(self, env_config_path, master_config_path, list_file_path, cli_tables, logger, global_ts, run_id, date_folder=None, main_path=None):
        self.logger = logger
        
        # 1. Load Environment Config
        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.local_temp_dir = os.path.join(main_path, 'temp')
        self.nas_dest_base = os.path.join(main_path, 'output')
        self.log_dir = os.path.join(main_path, 'log')
        self.metadata_base_dir = None
        self.config_master_file_path = master_config_path
        self.mapping_file_path = None
        self.gp_db = ''
        self.thai_mapping_table = ''
        self.thai_mapping_export_path = ''
        self.thai_dict = {}
        self.succeed_path = None
        self.global_ts = global_ts
        self.run_id = run_id

        # Default Number Configurations
        self.env_params = {
            'default_numeric_p': 38, 'default_numeric_s': 10,
            'cast_real_p': 24, 'cast_real_s': 6,
            'cast_double_p': 38, 'cast_double_s': 15,
            'round_numeric': 10, 'round_real': 5, 'round_double': 14
        }
        
        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # skip Header
                    if not line or line.startswith('#'):
                        continue

                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        if key == 'local_temp_dir': self.local_temp_dir = value
                        elif key == 'nas_destination': self.nas_dest_base = value
                        elif key == 'log_dir': self.log_dir = value
                        elif key == 'metadata_base_dir': self.metadata_base_dir = value
                        elif key == 'config_master_file_path' and not self.config_master_file_path: self.config_master_file_path = value
                        elif key == 'mapping_file_path': self.mapping_file_path = value
                        elif key == 'gp_db': self.gp_db = value
                        elif key == 'thai_mapping_table': self.thai_mapping_table = value
                        elif key == 'thai_mapping_export_path': self.thai_mapping_export_path = value
                        elif key == 'succeed_path': self.succeed_path = value
                        elif key == 'list_datatype_conv_only_no_len': self.list_datatype_conv_only_no_len = [v.strip().lower() for v in value.split(',')]
                        elif key in self.env_params:
                            self.env_params[key] = int(value)
                
            # Create temp dir if not exists
            if self.local_temp_dir is None:
                log_msg = "local_temp_dir is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            else:
                self.local_temp_dir = os.path.join(self.local_temp_dir, date_folder)
                if not os.path.exists(self.local_temp_dir):
                    os.makedirs(self.local_temp_dir)

            # Create output destination dir if not exists
            if self.nas_dest_base is None:
                self.logger.error("nas_destination is not defined in env_config.txt")
                raise
            else:
                self.nas_dest_base = os.path.join(self.nas_dest_base, date_folder)
            
            if self.log_dir is None:
                log_msg = "log_dir is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            if self.metadata_base_dir is None:
                log_msg = "metadata_base_dir is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            if self.gp_db is None:
                log_msg = "gp_db is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            if self.thai_mapping_table is None:
                log_msg = "thai_mapping_table is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            if self.succeed_path is None:
                log_msg = "succeed_path is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            if self.config_master_file_path is None:
                log_msg = "config_master_file_path is not defined in env_config.txt"
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            
            self.logger.info("Resolved local_temp_dir: {0}".format(self.local_temp_dir))
            self.logger.info("Resolved nas_dest_base: {0}".format(self.nas_dest_base))
            self.logger.info("Resolved log_dir: {0}".format(self.log_dir))
            self.logger.info("Resolved metadata_base_dir: {0}".format(self.metadata_base_dir))
            self.logger.info("Resolved mapping_file_path: {0}".format(self.mapping_file_path))
            self.logger.info("Resolved succeed_path: {0}".format(self.succeed_path))

        except Exception as e:
            self.logger.error("Failed to load environment config: {0}".format(e))
            raise

        self.type_mapping = {"SUM_MIN_MAX": [], "MIN_MAX": [], "MD5_MIN_MAX": []}
        if self.mapping_file_path and os.path.exists(self.mapping_file_path):
            self.logger.info("Loading Data Type Mapping from JSON: {0}".format(self.mapping_file_path))
            try:
                with open(self.mapping_file_path, 'r') as f:
                    self.type_mapping = json.load(f)
            except Exception as e:
                self.logger.error("Failed to parse mapping file: {0}. Using empty map.".format(e))
            
            # Check if datatype repeats in more than 1 method
            repeat_data_type = (
                (set(self.type_mapping['SUM_MIN_MAX']) & set(self.type_mapping['MIN_MAX'])) |
                (set(self.type_mapping['SUM_MIN_MAX']) & set(self.type_mapping['MD5_MIN_MAX'])) |
                (set(self.type_mapping['MIN_MAX']) & set(self.type_mapping['MD5_MIN_MAX']))
                )
            if repeat_data_type:
                str_repeat_data_type = ", ".join(sorted(repeat_data_type))
                log_msg = "Mapping file has repeat datatype: '{0}' in more than 1 method.".format(str_repeat_data_type)
                self.logger.error(log_msg)
                raise ValueError("Error: " + log_msg)
            
        else:
            self.logger.warning("Mapping file not found or path not defined: {0}".format(self.mapping_file_path))

        # 2. Determine Execution List
        self.execution_list = []
        if cli_tables:
            # Case 1: Use CLI Arguments
            self.logger.info("Using CLI arguments for table list.")
            # Expect format: DB|Schema.Table,DB2|Schema.Table
            tables = cli_tables.lower().split(',')
            for t in tables:
                try:
                    # Parse DB|Schema.Table
                    db_part, tbl_part = t.split('|')
                    sch_part, real_tbl = tbl_part.split('.')
                    
                    if db_part and sch_part and real_tbl:
                        self.execution_list.append({
                            'db': db_part.strip(),
                            'schema': sch_part.strip(),
                            'table': real_tbl.strip()
                        })
                    else:
                        self.logger.warning("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
                        continue
                        
                except ValueError:
                    self.logger.warning("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
        else:
            # Case 2: Use List File
            self.logger.info("Using list file: {0}".format(list_file_path))

            if os.path.exists(list_file_path) and os.path.getsize(list_file_path) > 0:
                try:
                    with open(list_file_path, 'r') as f:
                        for line in f:
                            line = line.lower().strip()
                            # skip Header
                            if not line or line.startswith('#'):
                                continue
                            
                            try:
                                # Format: DB|Schema.Table
                                db_part, tbl_part = line.split('|')
                                sch_part, real_tbl = tbl_part.split('.')
                                
                                if db_part and sch_part and real_tbl:
                                    self.execution_list.append({
                                        'db': db_part.strip(),
                                        'schema': sch_part.strip(),
                                        'table': real_tbl.strip()
                                    })
                                else:
                                    self.logger.warning("Skipping invalid line in list file: {0}".format(line))
                                    continue
                                
                            except ValueError:
                                self.logger.warning("Skipping invalid line in list file: {0}".format(line))
                except Exception as e:
                    self.logger.error("Failed to load list file: {0}".format(e))
                    raise
            else:
                self.logger.error("List file: {0} does not exist or is empty.".format(list_file_path))
                raise

        # distinct execution_list
        self.execution_list = list({(d['db'], d['schema'], d['table']): d for d in self.execution_list}.values())
        
        if self.gp_db and self.thai_mapping_table and self.thai_mapping_export_path:
            self._export_thai_mapping()
            self._load_thai_mapping()

        # 3. Load Master Config (Lookup Dictionary)
        self.master_data = {}
        self.logger.info("Loading master config: {0}".format(self.config_master_file_path))
        try:
            with open(self.config_master_file_path, 'r') as f:
                reader = csv.reader(f, delimiter='|')
                for line in reader:
                    # Format: DB | SCHEMA | table | manual_num_col
                    if len(line) < 4:
                        continue
                    db, sch, tbl, m_num = [x.strip() for x in line[:4]]
                    key = (db.strip().lower(), sch.strip().lower(), tbl.strip().lower())
                    self.master_data[key] = {
                        'manual_num': [x.strip().lower() for x in m_num.split(',') if x.strip().lower() != 'none' and x.strip()]
                    }
            self.logger.info("Loaded {0} tables from master config.".format(len(self.master_data)))
        except Exception as e:
            if not self.config_master_file_path or not os.path.exists(self.config_master_file_path):
                self.logger.error("Master config path not defined or not found: %r" % self.config_master_file_path)
            else:
                self.logger.error("Failed to load master config: {0}".format(e))
            raise

    def _export_thai_mapping(self):
        target_tables = set(["{0}.{1}".format(t['schema'], t['table']) for t in self.execution_list])
        thai_mapping_export_filenm = "thai_mapping_export_{0}_{1}.csv".format(self.global_ts, self.run_id)
        self.thai_mapping_export_full_path = os.path.join(self.thai_mapping_export_path, thai_mapping_export_filenm)
        if not target_tables: return
        
        in_clause = ",".join(["'{0}'".format(tbl) for tbl in target_tables])
        sql_query = "\\copy ( SELECT database_name, original_table_name, th_column_name, COALESCE(active_flag,'Y') active_flag FROM {0} WHERE original_table_name IN ({1}) ) TO '{2}' WITH CSV HEADER;".format(self.thai_mapping_table, in_clause, self.thai_mapping_export_full_path)
        
        filename = "query_export_thai_mapping_{0}.sql".format(self.global_ts)
        filepath = os.path.join(self.local_temp_dir, filename)

        with open(filepath, 'w') as f:
            f.write(sql_query)
            f.write("\n")
        
        self.logger.info("Generated SQL for Thai Mapping: {0}".format(filepath))
        cmd = ['psql', '-v', 'ON_ERROR_STOP=1', '-d', self.gp_db, '-f', filepath]
        self.logger.info("Executing PSQL... (DB: {0}) -> Output: {1}".format(self.gp_db, self.thai_mapping_export_full_path))
        self.logger.info("{0}".format(" ".join(cmd)))
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            try:
                stderr_text = stderr.decode('utf-8')
            except Exception:
                stderr_text = str(stderr)
                
            if process.returncode != 0:
                err_msg = "PSQL execution failed (Return Code: {0}) Error: {1}".format(process.returncode, stderr_text)
                self.logger.error(err_msg)
                raise RuntimeError(err_msg)
            if not (os.path.exists(self.thai_mapping_export_full_path) and os.path.getsize(self.thai_mapping_export_full_path) > 0):
                err_msg = "PSQL executed but output file is missing or is empty: {0} Error: {1}".format(self.thai_mapping_export_full_path, stderr_text)
                self.logger.error(err_msg)
                raise RuntimeError(err_msg)
            
        except Exception as e:
            err_msg = "Unexpected error when running psql: {0}".format(e).replace("\n", " ")
            self.logger.error(err_msg)
            raise RuntimeError(err_msg)

    def _load_thai_mapping(self):
        if not os.path.exists(self.thai_mapping_export_full_path): return
        try:
            with open(self.thai_mapping_export_full_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    db = row.get('database_name', '').strip().lower()
                    tbl_raw = row.get('original_table_name', '').strip().lower()
                    tbl = tbl_raw.split('.')[-1] if '.' in tbl_raw else tbl_raw
                    col = row.get('th_column_name', '').strip().lower()
                    flag = row.get('active_flag', '').strip().upper()
                    if db and tbl and col:
                        if (db, tbl) not in self.thai_dict: self.thai_dict[(db, tbl)] = {}
                        self.thai_dict[(db, tbl)][col] = flag
            self.logger.info("Loaded Thai mapping configuration for {0} tables.".format(len(self.thai_dict)))
        except Exception as e:
            self.logger.error("Error loading Thai mapping CSV: {0}".format(e))

# ==============================================================================
# 3. Handler Classes
# ==============================================================================

class QueryBuilder(object):
    def __init__(self, temp_dir, env_params, logger, global_ts):
        self.temp_dir = temp_dir
        self.env_params = env_params
        self.logger = logger
        self.global_ts = global_ts

    def _quote_json_val(self, sql_expr):
        return "COALESCE('\"' || ({0})::text || '\"', 'null')".format(sql_expr)
    
    def _build_num_expr(self, agg_func, col_expr, gp_type):
        gp_base = gp_type.split('(')[0].strip().lower()
        
        if gp_base == 'numeric' and '(' not in gp_type:
            p = self.env_params['default_numeric_p']
            s = self.env_params['default_numeric_s']
            r = self.env_params['round_numeric']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        elif gp_base == 'double precision':
            p = self.env_params['cast_double_p']
            s = self.env_params['cast_double_s']
            r = self.env_params['round_double']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        elif gp_base == 'real':
            p = self.env_params['cast_real_p']
            s = self.env_params['cast_real_s']
            r = self.env_params['round_real']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        else:
            return "{0}({1})".format(agg_func, col_expr)
        
    def build_json_query(self, db, schema, table, categorized_cols, insert_logic_dict):
        try:
            full_table_name = "{0}.{1}.{2}".format(db, schema, table)
            metric_fragments = []

            # Combine unique columns per solution category
            all_num_cols = set(categorized_cols['SUM_MIN_MAX'] + categorized_cols['MANUAL_NUM'])
            all_date_cols = set(categorized_cols['MIN_MAX'])
            all_cpx_cols = set(categorized_cols['MD5_MIN_MAX'])

            num_fragments = []
            date_fragments = []
            cpx_fragments = []

            # 1. NUMBER Solution
            for col in sorted(all_num_cols):
                gp_type = categorized_cols['TYPE_MAP'].get(col, 'numeric')
                base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                sum_expr = self._build_num_expr('SUM', base_expr, gp_type)
                min_expr = self._build_num_expr('MIN', base_expr, gp_type)
                max_expr = self._build_num_expr('MAX', base_expr, gp_type)
                frag = "'\"{0}\": {{' || '\"data_type\": \"{1}\", \"sum\": ' || {2} || ', \"min\": ' || {3} || ', \"max\": ' || {4} || '}}'".format(
                    col, gp_type, self._quote_json_val(sum_expr), self._quote_json_val(min_expr), self._quote_json_val(max_expr))
                num_fragments.append(frag)

            # 2. DATE Solution
            for col in sorted(all_date_cols):
                gp_type = categorized_cols['TYPE_MAP'].get(col, 'text') #fncheck#
                base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                min_expr = "MIN({0})::text".format(base_expr)
                max_expr = "MAX({0})::text".format(base_expr)
                frag = "'\"{0}\": {{' || '\"data_type\": \"{1}\", \"min\": ' || {2} || ', \"max\": ' || {3} || '}}'".format(
                    col, gp_type, self._quote_json_val(min_expr), self._quote_json_val(max_expr))
                date_fragments.append(frag)

            # 3. COMPLEX / THAI Solution
            for col in sorted(all_cpx_cols):
                gp_type = categorized_cols['TYPE_MAP'].get(col, 'text')
                base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                min_md5 = "MIN(MD5(COALESCE(({0})::text, '')))".format(base_expr)
                max_md5 = "MAX(MD5(COALESCE(({0})::text, '')))".format(base_expr)
                frag = "'\"{0}\": {{' || '\"data_type\": \"{1}\", \"min_md5\": ' || {2} || ', \"max_md5\": ' || {3} || '}}'".format(
                    col, gp_type, self._quote_json_val(min_md5), self._quote_json_val(max_md5))
                cpx_fragments.append(frag)

            method_groups = []
            if num_fragments:
                method_groups.append("'\"SUM_MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(num_fragments)))
            if date_fragments:
                method_groups.append("'\"MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(date_fragments)))
            if cpx_fragments:
                method_groups.append("'\"MD5_MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(cpx_fragments)))

            # Assemble Final SQL
            metrics_sql = " || ', ' || ".join(method_groups) if method_groups else "''"
            
            # Construct single JSON object string
            sql = (
                "SELECT '{{' || "
                " '\"table\": \"{0}\", ' || "
                " '\"source_type\": \"greenplum\", ' || "
                " '\"count\": ' || COUNT(*)::text || ', ' || "
                " '\"methods\": {{' || {1} || '}}' || "
                " '}}' "
                "FROM {2}.{3};"
            ).format(full_table_name, metrics_sql, schema, table)

            filename = "query_{0}_{1}_{2}_{3}.sql".format(db, schema, table, self.global_ts)
            filepath = os.path.join(self.temp_dir, filename)

            with open(filepath, 'w') as f:
                f.write(sql)
                f.write("\n")

            return filepath
        
        except Exception as e:
            self.logger.error("Error building JSON query: {0}".format(e))
            raise

class ShellHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def run_psql(self, sql_file, output_path, db_name=None):
        cmd = ['psql', '-v', 'ON_ERROR_STOP=1', '-q', '-t', '-A']

        if db_name:
            cmd.extend(['-d', db_name])
        cmd.extend(['-f', sql_file, '-o', output_path])

        self.logger.info("Executing PSQL... (DB: {0}) -> Output: {1}".format(db_name or 'Default', output_path))
        self.logger.info("{0}".format(" ".join(cmd)))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            try:
                stderr_text = stderr.decode('utf-8')
            except Exception:
                stderr_text = str(stderr)
                
            if process.returncode != 0:
                err_msg = "PSQL execution failed (Return Code: {0}) Error: {1}".format(process.returncode, stderr_text)
                self.logger.error(err_msg)
                raise RuntimeError(err_msg)
            if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
                err_msg = "PSQL executed but output file is missing or is empty: {0} Error: {1}".format(output_path, stderr_text)
                self.logger.error(err_msg)
                raise RuntimeError(err_msg)
            
        except Exception as e:
            err_msg = "Unexpected error when running psql: {0}".format(e).replace("\n", " ")
            self.logger.error(err_msg)
            raise RuntimeError(err_msg)

class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger
    def copy_to_nas(self, src, dest_dir):
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError:
                pass
        self.logger.info("Copying file from {0} to NAS: {1}".format(src, dest_dir))
        shutil.copy2(src, dest_dir)

# ==============================================================================
# 3. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, builder, shell, file_h, log_parser, tracker, logger, global_ts, status_file_filenm, status_file_locks, status_file_locks_lock):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.builder = builder
        self.shell = shell
        self.file_h = file_h
        self.log_parser = log_parser
        self.tracker = tracker
        self.logger = logger
        self.global_ts = global_ts
        self.daemon = True
        
        # csv file name
        self.status_file_filenm = status_file_filenm
        self.status_file_locks = status_file_locks
        self.status_file_locks_lock = status_file_locks_lock
        
        # pre-defined logging info
        self.start_time_tbl = None
        self.start_ts_tbl = None
        self.short_name = ""
        self.reconcile_method = []
        self.status = ""

    def _get_latest_metadata(self, db_name, schema_table):
        """Scans metadata_base_dir for the latest data_type and insert_logic files"""
        if not self.config.metadata_base_dir or not os.path.exists(self.config.metadata_base_dir):
            return None, None
        
        target_dir = os.path.join(self.config.metadata_base_dir, db_name)

        if not os.path.exists(target_dir):
            return None, None
            
        matches_dt = []
        matches_il = []
        
        # Walk through directories to find the most recent file
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith("_data_type.txt") and schema_table in file:
                    matches_dt.append(os.path.join(root, file))
                elif file.endswith("_insert_logic.txt") and schema_table in file:
                    matches_il.append(os.path.join(root, file))
                    
        latest_dt = sorted(matches_dt)[-1] if matches_dt else None
        latest_il = sorted(matches_il)[-1] if matches_il else None
        return latest_dt, latest_il

    def _check_manual_num(self, master_info, dt_file):
        self.logger.info("DEBUG: master_info = {0}".format(master_info))
        mapping = {}
        new_master_info = {'manual_num': []}
        manual_num_err = []
        with open(dt_file) as f:
            reader = csv.DictReader(f, delimiter="|")
            for row in reader:
                col_raw = row.get('gp_column_nm', '')
                dt = row.get('gp_datatype', '')
                if col_raw and dt:
                    mapping[col_raw.strip().lower()] = dt.strip().lower()
                
        for col in master_info.get('manual_num', []):
            lookup_col = col.strip().lower()
            datatype = mapping.get(lookup_col)
            if datatype is None:
                err_msg = "Column: {0} is not found in data type file".format(col)
                self.logger.error("[{0}] {1}".format(self.short_name, err_msg))
                manual_num_err.append(err_msg)
                continue
            
            if datatype in ['bigint', 'integer']:
                new_master_info['manual_num'].append(col)
            else:
                err_msg = "Column: {0} is NOT bigint or integer (data type = {1})".format(col, datatype)
                self.logger.error("[{0}] {1}".format(self.short_name, err_msg))
                manual_num_err.append(err_msg)
                
        self.logger.info("DEBUG: new_master_info = {0}".format(new_master_info))
        return new_master_info, manual_num_err

    def logging_status(self):
        # Define Temp target directory and file path
        base_log_dir = self.config.local_temp_dir

        # temp status directory: <temp>/<date>/stat_csv/<db>/<schema>
        status_dir = os.path.join(base_log_dir, 'stat_csv', self.db, self.schema)

        if not os.path.exists(status_dir):
            try:
                os.makedirs(status_dir)
            except Exception as e:
                self.logger.error("Cannot create status directory {0}: {1}".format(status_dir, e))

        status_file_full_path = os.path.join(status_dir, self.status_file_filenm)

        # Acquire or create a per-file lock (shared across all workers)
        with self.status_file_locks_lock:
            lock = self.status_file_locks.get(status_file_full_path)
            if lock is None:
                lock = threading.Lock()
                self.status_file_locks[status_file_full_path] = lock

        # Ensure required attributes exist
        short_name = getattr(self, "short_name", "")
        start_ts_tbl = getattr(self, "start_ts_tbl", "")
        start_time_tbl = getattr(self, "start_time_tbl", None)
        status = getattr(self, "status", "")
        error_message = getattr(self, "error_message", "")
        reconcile_method = getattr(self, "reconcile_method", [])
        json_output_path = getattr(self, "json_output_path_file", "")

        # Prepare timing
        end_time_tbl = time.time()
        end_ts_tbl = datetime.fromtimestamp(end_time_tbl).strftime("%Y-%m-%d %H:%M:%S")
        if start_time_tbl is None:
            duration_seconds = 0
        else:
            duration_seconds = int(end_time_tbl - start_time_tbl)

        # Format duration as HH:MM:SS
        hours, rem = divmod(duration_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        duration_tbl = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)

        # Reconcile method string
        reconcile_method_str = ",".join(set(reconcile_method)) if reconcile_method else ""

        row = [
            short_name,
            start_ts_tbl,
            end_ts_tbl,
            duration_tbl,
            reconcile_method_str,
            status,
            error_message,
            json_output_path,
            ""  # remark
        ]
        row = [s.encode('utf-8') if isinstance(s, unicode) else str(s) for s in row]
        
        # thread-safe write using per-file lock
        with lock:
            f = None
            try:
                f = open(status_file_full_path, "a")
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(row)
                f.flush()
            except Exception as e:
                self.logger.error("Failed to write status CSV {0} for {1}: {2}".format(status_file_full_path, short_name, e))
            finally:
                try:
                    if f:
                        f.close()
                except Exception:
                    pass

    def run(self):
        max_retries = 1
        
        while True:
            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            self.db = task['db'].strip().lower()
            self.schema = task['schema'].strip().lower()
            table = task['table'].strip().lower()
            full_name = "{0}.{1}.{2}".format(self.db, self.schema, table)
            # pre-defined logging info
            self.short_name = "{0}.{1}".format(self.schema, table)
            self.start_time_tbl = time.time()
            self.start_ts_tbl = datetime.fromtimestamp(self.start_time_tbl).strftime("%Y-%m-%d %H:%M:%S")
            self.reconcile_method = ['count']
            manual_num_err = []
            
            for attempt in range(1, max_retries + 1):
                # Reset error message for each attempt
                self.error_message = ""
                try:
                    self.tracker.update_worker_status(self.name, "[BUSY] {0} (Attempt {1})".format(self.short_name, attempt))
                    self.logger.info("Worker {0} started processing table: {1} (Attempt {2})".format(self.name, full_name, attempt))
                    start_t = time.time()

                    # Step 1: Check Log
                    log_row, log_msg = self.log_parser.get_latest_succeed_info(self.db, self.schema, table)
                    if not log_row:
                        raise ValueError(log_msg)

                    # Step 2: Config and Metadata
                    pre_master_info = self.config.master_data.get((self.db, self.schema, table), {'manual_num': []})
                    thai_config = self.config.thai_dict.get((self.db, table), {})
                    dt_file, il_file = self._get_latest_metadata(self.db, self.short_name)
                    self.logger.info("")
                    self.logger.info("Data type file: {0}".format(dt_file))
                    self.logger.info("Insert Logic file: {0}".format(il_file))
                    self.logger.info("")
                    missing_flag = False if dt_file else True

                    if missing_flag:
                        self.status = "FAILED"
                        self.error_message = "Not found data type file"
                        self.logger.error(self.error_message)
                        self.tracker.add_result(full_name, self.status, self.error_message)
                        break
                    else:
                        # Check manual_num column
                        master_info, manual_num_err = self._check_manual_num(pre_master_info, dt_file)
                        # Parse Logic
                        insert_logic_dict = {}
                        if il_file:
                            with open(il_file, 'r') as f:
                                cur_col = None
                                for line in f.read().splitlines():
                                    # skip Header
                                    if not line.strip() or line == "gp_column_nm;insert_logic":
                                        continue
                                    
                                    # match "column_name;logic"
                                    m = re.match(r'^([a-zA-Z0-9_]+);(.*)', line)
                                    if m:
                                        cur_col = m.group(1).strip().lower()
                                        insert_logic_dict[cur_col] = m.group(2)
                                    elif cur_col:
                                        insert_logic_dict[cur_col] += " " + line
                            
                            for col in insert_logic_dict:
                                logic = insert_logic_dict[col].replace('\\n', ' ').replace('\n', ' ')
                                insert_logic_dict[col] = re.sub(r'(?i)\s+AS\s+"?[a-zA-Z0-9_]+"?(?:\s*)$', '', logic).strip()
                        
                        # Categorize
                        cat_cols = {'SUM_MIN_MAX': [], 'MIN_MAX': [], 'MD5_MIN_MAX': [], 'TYPE_MAP': {}, 'MANUAL_NUM': master_info['manual_num']}
                        if dt_file:
                            with open(dt_file, 'r') as f:
                                reader = csv.DictReader(f, delimiter='|')
                                for row in reader:
                                    col_nm = row.get('gp_column_nm', '').strip().lower()
                                    gp_dt = row.get('gp_datatype', '').strip()
                                    
                                    if col_nm and gp_dt:
                                        cat_cols['TYPE_MAP'][col_nm] = gp_dt
                                        gp_base = gp_dt.split('(')[0].strip().lower()
                                        t_flag = thai_config.get(col_nm.lower())

                                        if t_flag == 'Y':
                                            gp_base = 'thai_col_flag_y'
                                        elif t_flag == 'N':
                                            gp_base = 'thai_col_flag_n'
                                            
                                        sum_map = [x.strip().lower() for x in self.config.type_mapping.get("SUM_MIN_MAX", [])]
                                        min_map = [x.strip().lower() for x in self.config.type_mapping.get("MIN_MAX", [])]
                                        md5_map = [x.strip().lower() for x in self.config.type_mapping.get("MD5_MIN_MAX", [])]

                                        if gp_base in sum_map:
                                            cat_cols['SUM_MIN_MAX'].append(col_nm)
                                            self.reconcile_method.append('number_sum_min_max')
                                        elif gp_base in min_map:
                                            cat_cols['MIN_MAX'].append(col_nm)
                                            self.reconcile_method.append('dttm_min_max')
                                        elif gp_base in md5_map:
                                            cat_cols['MD5_MIN_MAX'].append(col_nm)
                                            self.reconcile_method.append('md5_min_max')
                                        elif gp_base in self.config.list_datatype_conv_only_no_len and '(' not in gp_dt:
                                            cat_cols['MD5_MIN_MAX'].append(col_nm)
                                            self.reconcile_method.append('md5_min_max')

                        # Call JSON builder and save as .json file
                        sql_file = self.builder.build_json_query(self.db, self.schema, table, cat_cols, insert_logic_dict)
                        output_filename = "gp_{0}_{1}_{2}_{3}.json".format(self.db, self.schema, table, self.global_ts)
                        local_path = os.path.join(self.config.local_temp_dir, output_filename)
                        json_output_dir = os.path.join(self.config.nas_dest_base, self.db, self.schema)
                        self.json_output_path_file = os.path.join(json_output_dir, output_filename)
                        
                        self.shell.run_psql(sql_file, local_path, self.db)
                        self.file_h.copy_to_nas(local_path, json_output_dir)

                        if manual_num_err:
                            self.status = "FAILED"
                            self.error_message = " , ".join(manual_num_err)
                        else:
                            self.status = "SUCCEEDED"
                            self.error_message = "-"
                            
                        self.tracker.add_result(full_name, self.status, self.error_message)
                        self.tracker.log_step(full_name, time.time() - start_t)

                        break

                except ValueError as ve: 
                    self.status = "SKIPPED"
                    self.error_message = "{0}".format(ve).replace("\n", " ")
                    self.logger.error(self.error_message)
                    self.tracker.add_result(full_name, self.status, self.error_message)
                    break
                    
                except Exception as outer_e:
                    self.status = "FAILED"
                    if manual_num_err:
                        self.error_message = " , ".join(manual_num_err)
                    self.error_message = "{0} , {1}".format(self.error_message, outer_e).replace("\n", " ")
                    if attempt < max_retries:
                        self.logger.warning("Worker {0} Error on attempt {1} for {2}: {3}. Retrying in 3 seconds...".format(self.name, attempt, full_name, self.error_message))
                        time.sleep(3) # sleep before next attempt
                    else:
                        self.logger.error("Worker {0} Failed after {1} attempts for {2}: {3}".format(self.name, max_retries, full_name, self.error_message))
                        self.tracker.add_result(full_name, self.status, self.error_message)
                        
            # After finishing processing, write one CSV line:
            try:
                try:
                    self.logging_status()
                except Exception as e:
                    self.logger.error("Failed writing logging_status for {}: {}".format(full_name, e))
            finally:
                try:
                    self.queue.task_done()
                except Exception:
                    pass

class MonitorThread(threading.Thread):
    def __init__(self, tracker, num_workers, log_path, run_id):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.num_workers = num_workers
        self.log_path = log_path
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True
        self.run_id = run_id

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
        lines.append("================================================================================")
        lines.append(" GREENPLUM EXPORT MONITOR (Python 2.7 Parallel) ")
        lines.append("================================================================================")
        lines.append(" Run ID: {0}".format(self.run_id))
        lines.append(" Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct))
        lines.append(" Elapsed : {0:.0f}s".format(elapsed))
        lines.append("-" * 60)

        workers = sorted(self.tracker.worker_status.keys())
        for w_name in workers:
            status = self.tracker.worker_status.get(w_name, "Initializing...")
            line_str = " {0} : {1}".format(w_name, status)
            lines.append(line_str[:79]) 
        
        lines.append("-" * 60)
        lines.append(" Log File: {0}".format(self.log_path))
        lines.append(" Press Ctrl+C to abort.")

        if not self.first_print:
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        
        sys.stdout.write(output)
        sys.stdout.flush()

# ==============================================================================
# 4. Main Job Class
# ==============================================================================
class GreenplumExportJob(object):
    def __init__(self, args, logger, log_path, global_date_folder, global_ts, main_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.global_ts = global_ts
        self.tracker = ProcessTracker(logger)
        self.run_id = str(uuid.uuid4().hex)

        # Init Helpers
        self.config = Config(env_config_path=args.env, master_config_path=args.master, list_file_path=args.list, cli_tables=args.table_name, logger=logger, global_ts=self.global_ts, run_id=self.run_id, date_folder=global_date_folder, main_path=main_path)
        self.log_parser = LogParser(self.config.succeed_path, logger)
        self.builder = QueryBuilder(self.config.local_temp_dir, self.config.env_params, logger, self.global_ts)
        self.shell = ShellHandler(logger)
        self.file_h = FileHandler(logger)
        self.status_file_locks = {}
        self.status_file_locks_lock = threading.Lock()

        # Setup Queue
        self.job_queue = Queue.Queue()
        for task in self.config.execution_list:
            self.job_queue.put(task)

        self.tracker.set_total_tasks(len(self.config.execution_list))
        self.logger.info("Loaded {0} tasks into queue.".format(len(self.config.execution_list)))

    def run(self):
        input_name = os.path.splitext(os.path.basename(self.args.list))[0]
        status_file_filenm = "log_stat_rc_{0}_{1}.csv".format(input_name, self.global_ts)
            
        num_workers = int(self.args.concurrency)
        self.logger.info("Starting {0} workers...".format(num_workers))

        workers = []
        # Create & Start Workers
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.builder, self.shell, self.file_h, self.log_parser, self.tracker, self.logger, self.global_ts, status_file_filenm, self.status_file_locks, self.status_file_locks_lock)
            workers.append(w)
            w.start()

        # Start Monitor
        monitor = MonitorThread(self.tracker, num_workers, self.log_path, self.run_id)
        monitor.start()

        # Wait for Queue to be empty
        try:
            while self.tracker.completed_task < self.tracker.total_task:
                if not any(w.is_alive() for w in workers):
                    self.logger.error("All workers died unexpectedly! Aborting wait loop.")
                    break
                time.sleep(1)
            for w in workers:
                w.join()
        except KeyboardInterrupt:
            sys.stdout.write("\n\n>>> KEYBOARD INTERRUPT DETECTED! ABORTING SCRIPT... <<<\n\n")
            sys.stdout.flush()
            self.logger.warning("Keyboard Interrupt! User aborted the script.")
        finally:
            monitor.stop()
            monitor.join()
            
            # Copy stat file to NAS
            temp_stat_dir = os.path.join(self.config.local_temp_dir, 'stat_csv')
            files = glob.glob(os.path.join(temp_stat_dir, '*', '*', status_file_filenm))
            if files:
                self.logger.info("")
                self.logger.info("Start Copy Stat file = {0} file(s) to NAS...".format(len(files)))
                for f in files:
                    rel = os.path.relpath(os.path.dirname(f), temp_stat_dir)
                    dest_path = os.path.join(self.config.nas_dest_base, rel, 'stat_csv')
                    self.file_h.copy_to_nas(f, dest_path)
                self.logger.info("Copy Stat file = {0} file(s) to NAS successfully".format(len(files)))
                self.logger.info("")
                
            self.tracker.print_summary(self.log_path)
            

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Greenplum Data Export Tool')
    parser.add_argument('--env', default='env_config.txt', help='Name of env config file')
    parser.add_argument('--master', help='Name of master config file')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel workers (Default: 4)')
    
    # handle table and list
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list', default='list_table.txt', help='Name of list of tables file')
    group.add_argument('--table_name', help='Optional: Specific table to run (DB|Schema.Table)')

    args = parser.parse_args()

    def resolve_config_path(input_path, base_dir):
        if os.path.isabs(input_path):
            return input_path
        return os.path.join(base_dir, 'config', input_path)

    args.env = resolve_config_path(args.env, main_path)
    if args.master: args.master = resolve_config_path(args.master, main_path)
    args.list = resolve_config_path(args.list, main_path)

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")

    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else os.path.join(main_path, 'log')
    
    configured_master_file_path = peek_env_config(args.env, 'config_master_file_path')
    final_config_master_file_path = args.master if args.master else configured_master_file_path

    logger, log_path = setup_logging(final_log_dir, 'reconcile_query_greenplum', global_date_folder, global_ts)
    print(" Log File: {0}".format(log_path))
    logger.info("================================================================================")
    logger.info("Started with concurrency: {0}".format(args.concurrency))
    logger.info("Resolved env config path: {0}".format(args.env))
    logger.info("Resolved master config path: {0}".format(final_config_master_file_path))
    logger.info("Resolved list file path: {0}".format(args.list))
    logger.info("================================================================================")

    try:
        job = GreenplumExportJob(args, logger, log_path, global_date_folder, global_ts, main_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e))
        print("")
        print("End of Script.")
        print("exit 1")
        sys.exit(1)
        