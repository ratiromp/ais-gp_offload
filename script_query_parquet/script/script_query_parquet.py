#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import logging
import subprocess
import time
import argparse
import threading
import Queue
import glob
import re
import json
import shutil
import collections
from decimal import Decimal
from datetime import datetime
import uuid
import errno

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

    def set_total_task(self, total):
        self.total_task = total

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, duration=0.0, remark="-"):
            with self.lock:
                self.results.append({
                    'table': table_name, 
                    'status': status,
                    'duration': duration,
                    'remark': str(remark).replace('\n', ' ')
                })
                self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
        
    def print_summary(self, log_path, output_path):
        self.logger.info("="*100)
        self.logger.info("PARQUET JSON QUERY SUMMARY")
        self.logger.info("="*100)

        success_count = 0
        warning_count = 0
        failed_count = 0
        skipped_count = 0

        if not self.results:
            self.logger.info("No Partitions processed.")
        else:
            h_table = "Partition Name"
            h_status = "Status"
            h_dur = "Duration(s)"
            h_msg = "Remark"

            max_w_table = len(h_table)
            max_w_status = len(h_status)

            for r in self.results:
                if len(r['table']) > max_w_table: max_w_table = len(r['table'])
                if len(r['status']) > max_w_status: max_w_status = len(r['status'])

                if r['status'] == 'SUCCESS': success_count += 1
                elif r['status'] == 'WARNING': warning_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
                elif r['status'] == 'SKIPPED': skipped_count +=1
            
            w_table = max_w_table + 2
            w_status = max_w_status + 2

            row_fmt = "{0:<{wt}} | {1:<{ws}} | {2:<11} | {3}"
            header_line = row_fmt.format(h_table, h_status, h_dur, h_msg, wt=w_table, ws=w_status)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 90: sep_line = "-" * 90

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            for r in self.results:
                dur_str = "{0:.2f}".format(r.get('duration', 0.0))
                self.logger.info(row_fmt.format(
                    r['table'], r['status'], dur_str, r['remark'], wt=w_table, ws=w_status
                ))
            
            self.logger.info(sep_line)
            self.logger.info("Total: {0} | Success: {1} | Warning: {2} | Failed: {3} | Skipped: {4}".format(
                len(self.results), success_count, warning_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Write summary to output directory
        # summary_file = os.path.join(output_path, "reconcile_summary_{0}.txt".format(datetime.now().strftime("%Y%m%d_%H%M%S")))
        # try:
        #     with open(summary_file, 'w') as f:
        #         f.write("Total: {0}\nSuccess: {1}\nFailed: {2}\nSkipped: {3}\n".format(
        #             len(self.results), success_count, failed_count, skipped_count))
        # except Exception as e:
        #     self.logger.error("Could not write summary file to output: {0}".format(e))

        # Print Summary to Console
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total    : {0}".format(len(self.results)))
        print("Success  : {0}".format(success_count))
        print("Warning  : {0}".format(warning_count))
        print("Failed   : {0}".format(failed_count))
        print("Skipped  : {0}".format(skipped_count))
        print("Log File : {0}".format(log_path))
        # print("Output   : {0}".format(summary_file))
        print("="*80)

def setup_logging(log_dir, log_name="app", timestamp=None):
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Error: {1}".format(log_dir, e))
            log_dir = '.'

    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))
    logger = logging.getLogger("ParquetQueryBatch")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger, log_file

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class ConfigManager(object):
    def __init__(self, env_config_path, master_config_path, map_config_path, list_file_path, cli_tables, logger, global_date_folder, run_id, global_ts, main_path=None):
        self.logger = logger
        self.global_date_folder = global_date_folder
        self.run_id = run_id
        self.global_ts = global_ts

        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.succeed_path = ''
        self.hdfs_path = ''
        self.replace_path_from = ''
        self.replace_path_to = ''
        self.metadata_base_dir = ''
        self.datatype_mapping_path = ''
        self.gp_db = ''
        self.thai_mapping_table = ''
        self.thai_mapping_export_path = ''
        self.thai_dict = {}

        self.local_temp_dir = os.path.join(main_path, 'output')
        #self.nas_dest_base = os.path.join(main_path, 'output')
        #self.log_dir = os.path.join(main_path, 'log')


        self.env_params = {
            'default_numeric_p': 38, 'default_numeric_s': 10,
            'cast_real_p': 24, 'cast_real_s': 6,
            'cast_double_p': 38, 'cast_double_s': 15,
            'round_numeric': 10, 'round_real': 5, 'round_double': 14
        }

        # 1. Load Standalone Environment Config
        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key == 'local_temp_dir': self.local_temp_dir = value
                        elif key == 'succeed_path': self.succeed_path = value
                        elif key =='hdfs_path': self.hdfs_path = value
                        elif key == 'replace_path_from': self.replace_path_from = value
                        elif key == 'replace_path_to': self.replace_path_to = value
                        elif key == 'metadata_base_dir': self.metadata_base_dir = value
                        elif key == 'nas_destination': self.nas_destination = value
                        elif key == 'mapping_file_path': self.mapping_file_path = value
                        elif key == 'config_master_file_path': self.master_file_path = value
                        elif key == 'gp_db': self.gp_db = value
                        elif key == 'thai_mapping_table': self.thai_mapping_table = value
                        elif key == 'thai_mapping_export_path': self.thai_mapping_export_path = value
                        elif key in self.env_params:
                            self.env_params[key] = int(value)
        except IOError as e:
            self.logger.critical("Cannot find env_config file: {0}".format(e))
            raise

        # Create temp dir if not exists
        if self.nas_destination:
                self.nas_destination = os.path.join(self.nas_destination, self.global_date_folder)

        # Create temp dir if not exists
        if self.local_temp_dir is None:
            log_msg = "local_temp_dir is not defined in env_config.txt"
            self.logger.error(log_msg)
            raise ValueError("Error: " + log_msg)
        else:
            self.local_temp_dir = os.path.join(self.local_temp_dir, self.global_date_folder)
            if not os.path.exists(self.local_temp_dir):
                os.makedirs(self.local_temp_dir)
        
        self.logger.info("Resolved local_temp_dir: {0}".format(self.local_temp_dir))
        self.logger.info("Resolved nas_destination: {0}".format(self.nas_destination))
        #self.logger.info("Resolved log_dir: {0}".format(self.log_dir))
        self.logger.info("Resolved metadata_base_dir: {0}".format(self.metadata_base_dir))
        self.logger.info("Resolved mapping_file_path: {0}".format(self.mapping_file_path))
        self.logger.info("Resolved succeed_path: {0}".format(self.succeed_path))
        

        # 2. Load Target Tables (Moved up before Thai mapping logic to build IN clause)
        self.execution_list = []
        self.invalid_tables = []
        if cli_tables:
            self.logger.info("Using CLI arguments for table list.")
            tables = cli_tables.split(',')
            for t in tables:
                try:
                    db_part, tbl_part = t.split('|')
                    sch_part, real_tbl = tbl_part.split('.')
                    if db_part and sch_part and real_tbl:
                        self.execution_list.append({
                            'db': db_part.strip(),
                            'schema': sch_part.strip(),
                            'partition': real_tbl.strip()
                        })
                    else:
                        self.logger.error("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
                        self.invalid_tables.append({'table': t.strip(), 'reason': "Invalid format in argument: Expected DB|Schema.Table"})
                        continue
                except ValueError:
                    self.logger.error("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
                    self.invalid_tables.append({'table': t.strip(), 'reason': "Invalid format in argument: Expected DB|Schema.Table"})
        else:
            self.logger.info("Using list file: {0}".format(list_file_path))
            try:
                with open(list_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): continue
                        try:
                            db_part, tbl_part = line.split('|')
                            sch_part, real_tbl = tbl_part.split('.')
                            if db_part and sch_part and real_tbl:
                                self.execution_list.append({
                                    'db': db_part.strip(),
                                    'schema': sch_part.strip(),
                                    'partition': real_tbl.strip()
                                })
                            else:
                                self.logger.error("Skipping invalid line in list file: {0}. Expected DB|Schema.Table".format(line))
                                self.invalid_tables.append({'table': line, 'reason': "Invalid format in list file: Expected DB|Schema.Table"})
                                continue
                        except ValueError:
                            self.logger.warning("Skipping invalid line in list file: {0}. Expected DB|Schema.Table".format(line))
                            self.invalid_tables.append({'table': line, 'reason': "Invalid format in list file: Expected DB|Schema.Table"})
            except IOError as e:
                self.logger.critical("Cannot find list_table file: {0}".format(e))
                raise

        if not self.execution_list and not self.invalid_tables:
            self.logger.critical("CRITICAL_FAILED: Input table list is empty. Please provide valid tables via --table_name or list file.")
            raise ValueError("Input table list is empty or contains no rows.")

        # 3. Export and Load Thai Mapping via subprocess psql
        if self.gp_db and self.thai_mapping_table and self.thai_mapping_export_path:
            if not self._export_thai_mapping():
                raise RuntimeError("Failed to export Thai mapping from Greenplum. Aborting script.")
            self._load_thai_mapping()
        else:
            self.logger.warning("Thai mapping config missing in env_config. Skipping GP query.")

        # 4. Load Shared Master Config
        self.master_data = {}
        
        target_master_path = None
        if master_config_path:
            target_master_path = master_config_path
            pass
        elif getattr(self, 'master_file_path', ''):
            target_master_path = getattr(self, 'master_file_path', '')
            pass
        else:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            target_master_path = os.path.join(base_dir, 'config', 'config_master.txt')
            pass

        self.logger.info("Loading config_master (Shared): {0}".format(target_master_path))
        if os.path.isfile(target_master_path):
            try:
                with open(target_master_path, 'r') as f:
                    reader = csv.reader(f, delimiter='|')
                    for line in reader:
                        if len(line) < 4: continue
                        db, sch, tbl, m_num = [x.strip() for x in line[:4]]
                        self.master_data[(db, sch, tbl)] = {
                            'manual_num': [x.strip().lower() for x in m_num.split(',') if x.strip().lower() != 'none' and x.strip()]
                        }
            except Exception as e:
                self.logger.error("Failed to load master config: {0}".format(e))
                raise
        else:
            self.logger.warning("Target config_master file not found: {0}.".format(target_master_path))

        # 5. Load Shared Data Type Mapping
        self.type_mapping = {"SUM_MIN_MAX": [], "MIN_MAX": [], "MD5_MIN_MAX": []}
        target_map_path = getattr(self, 'mapping_file_path', '') or map_config_path
        self.logger.info("Loading data_type_mapping (Shared): {0}".format(map_config_path))
        try:
            with open(target_map_path, 'r') as f:
                self.type_mapping = json.load(f)
        except Exception as e:
            self.logger.error("Failed to load JSON mapping: {0}".format(e))
        #### Check if datatype duplicate between method ####
        repeat_data_type = (
            (set(self.type_mapping['SUM_MIN_MAX']) & set(self.type_mapping['MIN_MAX'])) |
            (set(self.type_mapping['SUM_MIN_MAX']) & set(self.type_mapping['MD5_MIN_MAX'])) |
            (set(self.type_mapping['MIN_MAX']) & set(self.type_mapping['MD5_MIN_MAX']))
        )
        if repeat_data_type:
            str_repeat_data_type = ", ".join(sorted(repeat_data_type))
            self.logger.error("Mapping file has repeat datatype: '{0}' in many method.".format(str_repeat_data_type))
            raise
        else:
            self.logger.info("Loading data_type_mapping (Shared): SUCCESS.")
    def _export_thai_mapping(self):
        target_tables = set(["{0}.{1}".format(t['schema'], t['partition']) for t in self.execution_list])
        thai_mapping_export_filenm = "thai_mapping_export_{0}_{1}.csv".format(self.global_ts, self.run_id)
        self.thai_mapping_export_full_path = os.path.join(self.thai_mapping_export_path, thai_mapping_export_filenm)
        if not target_tables:
            return True
        
        table_list = ["'{0}'".format(tbl) for tbl in target_tables]
        in_clause = ",".join(table_list)
        
        sql_query = "\\copy (SELECT database_name, original_table_name, th_column_name, COALESCE(active_flag,'Y') FROM {0} WHERE original_table_name IN ({1})) TO '{2}' WITH CSV HEADER;".format(
            self.thai_mapping_table, in_clause, self.thai_mapping_export_full_path)
        #self.logger.info("Generated SQL Query for Thai Mapping: {0}".format(sql_query))
        #cmd = [
        #    'psql',
        #    '-d', self.gp_db,
        #    '-c', sql_query
        #]

        filename = "query_export_thai_mapping_{0}_{1}.sql".format(self.global_ts, self.run_id)
        filepath = os.path.join(self.thai_mapping_export_path, filename)

        with open(filepath, 'w') as f:
            f.write(sql_query)
            f.write("\n")

        self.logger.info("Generated SQL for Thai Mapping: {0}".format(filepath))
        cmd = ['psql', '-v', 'ON_ERROR_STOP=1', '-d', self.gp_db, '-f', filepath]
        self.logger.info("Executing PSQL... (DB: {0}) -> Output: {1}".format(self.gp_db, self.thai_mapping_export_full_path))
        self.logger.info("{0}".format(" ".join(cmd)))

        self.logger.info("Executing subprocess to export Thai mapping to {0}".format(self.thai_mapping_export_full_path))
        #try:
        #    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #    stdout, stderr = process.communicate()
        #    
        #    if process.returncode == 0:
        #        self.logger.info("Export Thai mapping via psql successful.")
        #        return True
        #    else:
        #        self.logger.error("psql export failed:\n{0}".format(stderr.decode('utf-8', errors='ignore')))
        #        return False
        #except Exception as e:
        #    self.logger.error("Failed to execute psql subprocess: {0}".format(e))
        #    return False
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            try:
                stderr_text = stderr.decode('utf-8')
            except Exception:
                stderr_text = str(stderr)
            
            if process.returncode == 0:
                self.logger.info("Export Thai mapping via psql successful.")
                return True
            if process.returncode != 0:
                err_msg = "PSQL execution failed (Return Code: {0}) Error: {1}".format(process.returncode, stderr_text)
                self.logger.error(err_msg)
                return False
            if not (os.path.exists(self.thai_mapping_export_full_path) and os.path.getsize(self.thai_mapping_export_full_path) > 0):
                err_msg = "PSQL executed but output file missing or empty: {0} Error: {1}".format(self.thai_mapping_export_full_path, stderr_text)
                self.logger.error(err_msg)
                return False
            
        except Exception as e:
            err_msg = "Unexpected error when running psql: {0}".format(e).replace("\n", " ")
            self.logger.error(err_msg)
            return False
        
    #def _load_thai_mapping(self):
    #    if not os.path.exists(self.thai_mapping_export_full_path):
    #        self.logger.warning("Thai mapping file not found at {0}".format(self.thai_mapping_export_full_path))
    #        return
    #    
    #    try:
    #        with open(self.thai_mapping_export_full_path, 'r') as f:
    #            reader = csv.DictReader(f)
    #            for row in reader:
    #                db = row.get('database_name', '').strip().lower()
    #                tbl_raw = row.get('original_table_name', '').strip().lower()
    #                if '.' in tbl_raw:
    #                    tbl = tbl_raw.split('.')[-1]
    #                else:
    #                    tbl = tbl_raw
    #                col = row.get('th_column_name', '').strip().lower()
    #                flag = row.get('active_flag', '').strip().upper()
    #                if db and tbl and col:
    #                    if (db, tbl) not in self.thai_dict:
    #                        self.thai_dict[(db, tbl)] = {}
    #                    self.thai_dict[(db, tbl)][col] = flag
    #        self.logger.info("Loaded Thai mapping configuration for {0} tables.".format(len(self.thai_dict)))
    #    except Exception as e:
    #        self.logger.error("Error loading Thai mapping CSV: {0}".format(e))

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
# 3. Handler & Helper Classes
# ==============================================================================

class SparkQueryBuilder(object):
    def __init__(self, env_params, type_mapping, logger):
        self.env_params = env_params
        self.type_mapping = type_mapping
        self.logger = logger

    def _build_num_expr(self, agg_func, col, gp_type):
        """ REQ 5.1: Numeric Type handling with precision, scale and rounding """
        gp_base = gp_type.split('(')[0].strip().lower()
        
        # 1. Defaults
        p, s, r = 38, 10, 10 
        is_exact_int = False

        if gp_base == 'numeric' and '(' in gp_type:
            try:
                ps = gp_type.split('(')[1].replace(')', '').split(',')
                p = int(ps[0].strip())
                s = int(ps[1].strip())
                r = s # SLA: Rounding equals scale if defined
            except: pass
        elif gp_base == 'numeric':
            p, s, r = self.env_params['default_numeric_p'], self.env_params['default_numeric_s'], self.env_params['round_numeric']
        elif gp_base == 'double precision':
            p, s, r = self.env_params['cast_double_p'], self.env_params['cast_double_s'], self.env_params['round_double']
        elif gp_base == 'real':
            p, s, r = self.env_params['cast_real_p'], self.env_params['cast_real_s'], self.env_params['round_real']
        elif gp_base in ['smallint', 'integer', 'bigint']:
            is_exact_int = True

        # 2. Build SparkSQL String Expression
        if is_exact_int:
            # Prevents overflow by casting to bigint before sum
            return "CAST({0}(CAST(`{1}` AS BIGINT)) AS STRING)".format(agg_func, col)
        else:
            # Apply Cast -> Aggregate -> Round -> Cast to String
            return "CAST(ROUND({0}(CAST(`{1}` AS DECIMAL({2},{3}))), {4}) AS STRING)".format(
                agg_func, col, p, s, r
            )

    # def _build_date_expr(self, agg_func, col, gp_type):
    #     """ REQ 5.2: Date/Time Type with Regex and Named Struct logic """
    #     # Step 1: Base Regex Cleansing (Handles year > 9999 and 24:00:00)
    #     clean_str = "regexp_replace(regexp_replace(CAST(`{0}` AS STRING), '^[0-9]{{5,}}-', '9999-'), '24:00:00', '23:59:59')".format(col)

    #     # Step 2: Specific Data Type Parsing (Determine Timestamp Format)
    #     ts_parse = ""
    #     gp_base = gp_type.split('(')[0].strip().lower()
    #     if 'timestamp' in gp_base:
    #         ts_parse = "CASE WHEN `{0}` LIKE '% BC' THEN to_timestamp({1}, 'yyyy-MM-dd HH:mm:ss G') ELSE to_timestamp({1}) END".format(col, clean_str)
    #     elif 'date' in gp_base:
    #         ts_parse = "CASE WHEN `{0}` LIKE '% BC' THEN to_timestamp({1}, 'yyyy-MM-dd G') ELSE to_timestamp({1}, 'yyyy-MM-dd') END".format(col, clean_str)
    #     elif 'time' in gp_base:
    #         ts_parse = "to_timestamp(concat('1970-01-01 ', {0}))".format(clean_str)
    #     else:
    #         ts_parse = "to_timestamp({0})".format(clean_str)

    #     # Step 3: Build Struct for Aggregation
    #     struct_expr = "CASE WHEN {0} IS NULL THEN NULL ELSE named_struct('ts', {0}, 'val', CAST(`{1}` AS STRING)) END".format(ts_parse, col)

    #     # Step 4: Extract Aggregated Value
    #     return "{0}({1}).val".format(agg_func, struct_expr)
    
    def _build_date_expr(self, agg_func, col, gp_type):
        # Step 1: Base Regex Cleansing
        clean_str = "CAST(`{0}` AS STRING)".format(col)
        clean_str = "regexp_replace({0}, '^[0-9]{{5,}}-', '9999-')".format(clean_str)
        clean_str = "regexp_replace({0}, '24:00:00', '23:59:59')".format(clean_str)
        clean_str = "regexp_replace({0}, '([+-][0-9]{{2}}:[0-9]{{2}}):[0-9]{{2}}', '$1')".format(clean_str)

        # Step 2: Specific Data Type Parsing
        ts_parse = ""
        gp_base = gp_type.split('(')[0].strip().lower()
        
        if gp_base == "timestamp with time zone":
            bc_clean = "regexp_replace({0}, '[+-][0-9]{{2}}(:[0-9]{{2}})? BC$', ' BC')".format(clean_str)
            ts_parse = "CASE WHEN `{0}` LIKE '% BC' THEN to_timestamp({1}, 'yyyy-MM-dd HH:mm:ss G') ELSE to_timestamp({2}, 'yyyy-MM-dd HH:mm:ssX') END".format(col, bc_clean, clean_str)
            
        elif gp_base in ("timestamp without time zone", "timestamp"):
            ts_parse = "CASE WHEN `{0}` LIKE '% BC' THEN to_timestamp({1}, 'yyyy-MM-dd HH:mm:ss G') ELSE to_timestamp({1}, 'yyyy-MM-dd HH:mm:ss') END".format(col, clean_str)
            
        elif gp_base == "date":
            ts_parse = "CASE WHEN `{0}` LIKE '% BC' THEN to_timestamp({1}, 'yyyy-MM-dd G') ELSE to_timestamp({1}, 'yyyy-MM-dd') END".format(col, clean_str)
            
        elif gp_base == "time with time zone":
            clean_str = "concat('1970-01-01 ', {0})".format(clean_str)
            ts_parse = "to_timestamp({0}, 'yyyy-MM-dd HH:mm:ssX')".format(clean_str)
            
        elif gp_base == "time without time zone":
            clean_str = "concat('1970-01-01 ', {0})".format(clean_str)
            ts_parse = "to_timestamp({0}, 'yyyy-MM-dd HH:mm:ss')".format(clean_str)
        else:
            ts_parse = "to_timestamp({0})".format(clean_str)

        # Step 3: Build Struct for Aggregation (Add 'yr' for safe extreme date sorting)
        if gp_base in ("time with time zone", "time without time zone"):
            struct_expr = "CASE WHEN {0} IS NOT NULL THEN named_struct('ts', {0}, 'val', CAST(`{1}` AS STRING)) END".format(ts_parse, col)
        else:
            year_expr = "CAST(regexp_extract(CAST(`{0}` AS STRING), '^([0-9]+)-', 1) AS BIGINT) * CASE WHEN CAST(`{0}` AS STRING) LIKE '% BC' THEN -1 ELSE 1 END".format(col)
            struct_expr = "CASE WHEN {0} IS NOT NULL THEN named_struct('yr', {2}, 'ts', {0}, 'val', CAST(`{1}` AS STRING)) END".format(ts_parse, col, year_expr)

        # Step 4: Extract Aggregated Value
        return "{0}({1}).val".format(agg_func, struct_expr)

    def _build_md5_expr(self, agg_func, col):
        """ REQ 5.3: MD5 Type without Trim """
        # Null-safe cast to string, then md5, then agg
        return "CAST({0}(MD5(COALESCE(CAST(`{1}` AS STRING), ''))) AS STRING)".format(agg_func, col)

    def build_agg_exprs(self, cat_cols):
        """
        Takes Dictionary of categorized columns and returns Spark DataFrame expressions
        Example cat_cols: {'SUM_MIN_MAX': ['col1'], 'MIN_MAX': ['col2'], 'MD5_MIN_MAX': ['col3'], 'TYPE_MAP': {'col1':'numeric(18,2)'}}
        """
        exprs = [F.expr("CAST(COUNT(*) AS STRING)").alias("count")]

        all_num_cols = set(cat_cols['SUM_MIN_MAX'] + cat_cols['MANUAL_NUM'])
        all_date_cols = set(cat_cols['MIN_MAX'])
        all_cpx_cols = set(cat_cols['MD5_MIN_MAX'])

        # Discard Logic (Prevent Duplicates)
        #for col in all_num_cols:
        #    all_date_cols.discard(col)
        #    all_cpx_cols.discard(col)
        #for col in all_date_cols:
        #    all_cpx_cols.discard(col)
        
        # 1. SUM_MIN_MAX
        for col in all_num_cols:
            gp_type = cat_cols['TYPE_MAP'].get(col, 'numeric')
            exprs.append(F.expr(self._build_num_expr('SUM', col, gp_type)).alias("SUM_MIN_MAX|{0}|sum".format(col)))
            exprs.append(F.expr(self._build_num_expr('MIN', col, gp_type)).alias("SUM_MIN_MAX|{0}|min".format(col)))
            exprs.append(F.expr(self._build_num_expr('MAX', col, gp_type)).alias("SUM_MIN_MAX|{0}|max".format(col)))

        # 2. MIN_MAX
        for col in all_date_cols:
            gp_type = cat_cols['TYPE_MAP'].get(col, 'timestamp')
            exprs.append(F.expr(self._build_date_expr('MIN', col, gp_type)).alias("MIN_MAX|{0}|min".format(col)))
            exprs.append(F.expr(self._build_date_expr('MAX', col, gp_type)).alias("MIN_MAX|{0}|max".format(col)))

        # 3. MD5_MIN_MAX
        for col in all_cpx_cols:
            exprs.append(F.expr(self._build_md5_expr('MIN', col)).alias("MD5_MIN_MAX|{0}|min_md5".format(col)))
            exprs.append(F.expr(self._build_md5_expr('MAX', col)).alias("MD5_MIN_MAX|{0}|max_md5".format(col)))

        return exprs

class LogParser(object):
    def __init__(self, succeed_base_path, logger):
        self.succeed_base_path = succeed_base_path
        self.logger = logger
        self.cache = {} 
        self.lock = threading.Lock()

    def get_latest_succeed_info(self, db, schema, partition):
        cache_key = "{0}_{1}".format(db, schema)
        
        with self.lock:
            if cache_key not in self.cache:
                self.logger.info("[LogParser] Building memory cache for DB: {0}, Schema: {1} ...".format(db, schema))
                self.cache[cache_key] = {}
                
                
                #search_pattern = os.path.join(self.succeed_base_path, db, "*", "offloadgp_stat_succeeded.{0}.csv".format(schema))
                search_pattern = os.path.join(self.succeed_base_path, db, "*", "offloadgp_stat.{0}.csv".format(schema))
                # search_pattern = os.path.join(self.succeed_base_path, db, "backup_old_file", "*", "offloadgp_stat_succeeded.{0}.csv".format(schema))
                self.logger.info("[LogParser] Searching for succeed log using pattern: {0}".format(search_pattern))
                matched_files = sorted(glob.glob(search_pattern), reverse=True)

                if not matched_files:
                    self.logger.critical("[LogParser] Log files not found for pattern: {0}.".format(search_pattern))
                    raise RuntimeError("CRITICAL_FAILED: Log files not found for pattern: {0}".format(search_pattern))
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
                                    status = row.get('Run_Status', '')

                                    if gp_tbl == 'Greenplum_Tbl':
                                        continue
                                    
                                    if gp_tbl and status == 'SUCCEEDED':
                                        if gp_tbl not in self.cache[cache_key]:
                                            self.cache[cache_key][gp_tbl] = row
                                        else:
                                            current_ts = self.cache[cache_key][gp_tbl].get('End_Timestamp_Script', '')
                                            new_ts = row.get('End_Timestamp_Script', '')
                                            if new_ts > current_ts:
                                                self.cache[cache_key][gp_tbl] = row
                        except Exception as e:
                            self.logger.warning("[LogParser] Error parsing log file {0}: {1}".format(log_file, e))
                self.logger.info("[LogParser] Cache build completed. Total {0} tables cached.".format(len(self.cache[cache_key])))

        target_table_with_schema = "{0}.{1}".format(schema, partition)
        
        latest_row = self.cache[cache_key].get(partition) or self.cache[cache_key].get(target_table_with_schema)
        # self.logger.info(latest_row)
        if latest_row and latest_row.get('Run_Status') == 'SUCCEEDED':
            self.logger.info("[LogParser] Found latest SUCCEEDED record for {0} from Cache. Target Parquet: {1}".format(
                partition, latest_row.get('File_Path', 'N/A')))
            return latest_row, "Found SUCCEEDED record"
        elif latest_row and latest_row.get('Run_Status') == 'FAILED':
            self.logger.warning("[LogParser] Skip table {0}. Latest Export status is not SUCCEEDED.".format(target_table_with_schema))
            return None, "Latest Export status is not SUCCEEDED"
        else:
            self.logger.warning("[LogParser] Status is not SUCCEEDED in cache for {0}".format(partition))
            return None, "Status is not SUCCEEDED in any log files"
    
class HDFSHandler(object):
    def __init__(self, spark_session, logger):
        self.logger = logger
        self.spark = spark_session

        self.sc = self.spark.sparkContext
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)
        self.Path = self.sc._jvm.org.apache.hadoop.fs.Path

    def sync_parquet(self, local_file_path, hdfs_dest_path):
        self.logger.info("[HDFSHandler] Evaluating HDFS sync. Local: {0} -> HDFS: {1}".format(local_file_path, hdfs_dest_path))
        try:
            local_mtime = os.path.getmtime(local_file_path)
        except OSError as e:
            raise ValueError("SKIPPED: Local path not found or inaccessible: {0}".format(local_file_path))
        
        has_parquet = False
        if os.path.isdir(local_file_path):
            for root, dirs, files in os.walk(local_file_path):
                if any(f.endswith('.parquet') for f in files):
                    has_parquet = True
                    break
        else:
            if local_file_path.endswith('.parquet'):
                has_parquet = True
                
        if not has_parquet:
            raise ValueError("SKIPPED: No parquet files found in local path (or sub-folders): {0}".format(local_file_path))

        local_path_str = "file:///" + os.path.abspath(local_file_path).replace("\\", "/").lstrip("/")
        local_path_obj = self.Path(local_path_str)
        hdfs_path_obj = self.Path(hdfs_dest_path)

        needs_upload = False

        if not self.fs.exists(hdfs_path_obj):
            needs_upload = True
        else:
            try:
                file_status = self.fs.getFileStatus(hdfs_path_obj)
                hdfs_mtime = file_status.getModificationTime() / 1000.0

                if local_mtime > hdfs_mtime:
                    needs_upload = True
            except Exception as e:
                self.logger.warning("Could not get HDFS file status via JVM. Forcing upload. Error: {0}".format(e))
                needs_upload = True

        if needs_upload:
            self.logger.info("[HDFSHandler] Proceeding to upload Parquet to HDFS: {0}".format(hdfs_dest_path))
            try:
                parent_path = hdfs_path_obj.getParent()
                if parent_path and not self.fs.exists(parent_path):
                    self.fs.mkdirs(parent_path)
                
                self.fs.copyFromLocalFile(False, True, local_path_obj, hdfs_path_obj)
            except Exception as e:
                raise RuntimeError("HDFS copyFromLocalFile via JVM failed: {0}".format(e))
        else:
            self.logger.info("[HDFSHandler] Parquet file is up-to-date. Skipping HDFS upload.")
        return True

class MetadataFetcher(object):
    def __init__(self, base_dir, logger):
        self.base_dir = base_dir
        self.logger = logger

    def fetch_data_types(self, db_name, table_name):
        if not self.base_dir or not os.path.exists(self.base_dir): return None
        
        target_dir = os.path.join(self.base_dir, db_name)
        if not os.path.exists(target_dir): return None
            
        matches = [os.path.join(r, f) for r, _, fs in os.walk(target_dir) for f in fs if f.endswith("_data_type.txt") and table_name in f]
        latest_file = sorted(matches)[-1] if matches else None
        
        type_map = {}
        if latest_file:
            try:
                with open(latest_file, 'r') as f:
                    reader = csv.DictReader(f, delimiter='|')
                    for row in reader:
                        col_nm = row.get('gp_column_nm', '').strip()
                        gp_dt = row.get('gp_datatype', '').strip()
                        
                        if col_nm and gp_dt:
                            type_map[col_nm.lower()] = gp_dt
            except Exception as e:
                self.logger.warning("Error reading data type file {0}: {1}".format(latest_file, e))
                return None
        return type_map

class HiveLogger(object):
    def __init__(self, spark_session, logger):
        self.spark = spark_session
        self.logger = logger
        self.table_header = "output_reconcile_reconcile_query_parquet"
        self.insert_lock = threading.Lock()

    def log_execution_status(self, execution_id, db, schema, table, partition, start_ts, end_ts, duration, status, remark):
        safe_remark = str(remark).replace("'", "") if remark else ""
        insert_sql = """
            INSERT INTO TABLE {0}
            VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', cast('{6}' as timestamp), cast('{7}' as timestamp), cast({8} as decimal(18,2)), '{9}', '{10}')
        """.format(self.table_header, execution_id, db, schema, table, partition, start_ts.strftime('%Y-%m-%d %H:%M:%S'), end_ts.strftime('%Y-%m-%d %H:%M:%S'), duration, status, safe_remark)
        try:
            with self.insert_lock: self.spark.sql(insert_sql)
        except Exception as e:
            self.logger.warning("Hive Log Insert Failed: {0}".format(e))


#class FileHandler(object):
#    def __init__(self, logger):
#        self.logger = logger
#    def copy_to_nas(self, src, dest_dir):
#        if not os.path.exists(dest_dir):
#            try:
#                os.makedirs(dest_dir)
#            except OSError:
#                pass
#        self.logger.info("Copying file from {0} to NAS: {1}".format(src, dest_dir))
#        shutil.copy2(src, dest_dir)


class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def copy_to_nas(self, src, dest_dir):
        # 1. Check if source exists first to avoid confusing errors
        if not os.path.exists(src):
            self.logger.error("Source file not found: {0}".format(src))
            return False

        # 2. Hardened directory creation
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError as e:
                # Ignore if directory was created by another process/thread
                if e.errno != errno.EEXIST:
                    self.logger.error("Critical: Cannot create NAS directory {0}. Error: {1}".format(dest_dir, e))
                    return False
            except Exception as e:
                self.logger.error("Unexpected error creating NAS directory: {0}".format(e))
                return False

        # 3. Perform copy with metadata preservation
        try:
            self.logger.info("Copying file from {0} to NAS: {1}".format(src, dest_dir))
            shutil.copy2(src, dest_dir)
            return True
        except Exception as e:
            self.logger.error("Failed to copy file to NAS: {0}".format(e))
            return False

# ==============================================================================
# 4. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, log_parser, hdfs_h, meta_fetcher, query_builder, hive_logger, spark, tracker, logger, execution_id, global_ts, out_path, abort_event, run_id, status_file_filenm, status_file_locks, status_file_locks_lock, file_h):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.log_parser = log_parser
        self.hdfs_h = hdfs_h
        self.meta_fetcher = meta_fetcher
        self.query_builder = query_builder
        self.hive_logger = hive_logger
        self.spark = spark
        self.tracker = tracker
        self.logger = logger
        self.execution_id = execution_id
        self.global_ts = global_ts
        self.out_path = out_path
        self.abort_event = abort_event
        self.daemon = True
        self.run_id = run_id
        self.file_h = file_h

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
        self.error_message = ""

    def _copy_file_to_nas(self, local_file_path, db, schema, target_file_name):
        nas_dir = os.path.join(self.config.nas_destination, db, schema)
        nas_file_path = os.path.join(nas_dir, target_file_name)

        try:
            if not os.path.exists(nas_dir):
                try: 
                    os.makedirs(nas_dir)
                except OSError as e:
                    #import errno
                    if e.errno != errno.EEXIST:
                        raise

            shutil.copy2(local_file_path, nas_file_path)
            return True, None

        except Exception as e:
            return False, str(e)

    def logging_status(self, status, remark=""):
        # 1. Define directory path
        # temp status directory: /output/<date>/stat_csv/<db>/<schema>
        status_dir = os.path.join(self.config.local_temp_dir, 'stat_csv', self.db, self.schema)
        self.logger.info("[{0}] Logging status to directory: {1}".format(self.name, status_dir))
        #self.status = status

        remark = "-" if status.upper() == "SUCCESS" else remark
        statused = "SUCCEEDED" if status.upper() == "SUCCESS" else status


        # 2. Directory creation with Race Condition handling
        if not os.path.exists(status_dir):
            try:
                os.makedirs(status_dir)
            except OSError as e:
                # errno.EEXIST is Error code 17 (File exists)
                # If the directory was created by another thread just now, ignore the error
                if e.errno != errno.EEXIST:
                    self.logger.critical("Cannot create directory {0}. Error: {1}".format(status_dir, e))
                    return
            except Exception as e:
                self.logger.error("Unexpected error creating directory: {0}".format(e))
                return
        self.logger.info("[{0}] Directory created successfully: {1}".format(self.name, status_dir))

        status_file_full_path = os.path.join(status_dir, self.status_file_filenm)

        # 3. Thread-safe lock acquisition (Optimized using setdefault)
        # Acquire or create a per-file lock (shared across all workers) to prevent race conditions
        with self.status_file_locks_lock:
            lock = self.status_file_locks.setdefault(status_file_full_path, threading.Lock())
        self.logger.info("[{0}] Acquired lock for file: {1}".format(self.name, status_file_full_path))

        # 4. Attribute retrieval with getattr
        short_name = getattr(self, "short_name", "")
        start_ts = getattr(self, "start_ts_tbl", "")
        #status = getattr(self, "status", "")
        #error_message = getattr(self, "error_message", "").replace('\n', ' ')
        json_output_path = getattr(self, "local_json_file", "")
        reconcile_method = getattr(self, "reconcile_method", [])
        self.logger.info("[{0}] Retrieved attributes for logging. Short Name: {1}, Start TS: {2}, Status: {3}".format(
            self.name, short_name, start_ts, statused
        ))

        # 5. Timing & Duration calculation
        curr_time = time.time()
        start_time = getattr(self, "start_time_tbl", None)

        if start_time is not None:
            duration_sec = int(max(0, curr_time - start_time))
        else:
            duration_sec = 0

        hours, rem = divmod(duration_sec, 3600)
        minutes, seconds = divmod(rem, 60)
        duration_str = "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)
        end_ts = datetime.fromtimestamp(curr_time).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info("[{0}] Calculated duration: {1}".format(self.name, duration_str))


        # 6. Reconcile method formatting
        reconcile_method_str = ",".join(set(reconcile_method)) if reconcile_method else ""

        # 7. Construct and encode row
        row = [
            short_name, 
            start_ts, end_ts, 
            duration_str, 
            reconcile_method_str, 
            statused, 
            remark, 
            json_output_path, 
            "" # remark
            ] 

        row = [s.encode('utf-8') if isinstance(s, unicode) else str(s) for s in row]

        self.logger.info("Prepared log row for {0}: {1}".format(self.name, row))

        # 8. Thread-safe file writing (Using binary mode 'ab' for Python 2.7)
        with lock:
            try:
                with open(status_file_full_path, "ab") as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                    writer.writerow(row)
            except Exception as e:
                self.logger.critical("Error writing to CSV: {0}".format(e))

    def _check_manual_num(self, master_info, type_map):
        self.logger.info("DEBUG: master_info = {0}".format(master_info))
        new_master_info = {'manual_num': []}
        manual_num_err = []
        
        for col in master_info.get('manual_num', []):
            lookup_col = col.strip().lower()
            datatype = type_map.get(lookup_col)
            if datatype is None:
                remark = "Column: {0} is not found in data type file".format(col)
                self.logger.error("[{0}] {1}".format(self.name, remark))
                manual_num_err.append(remark)
                continue
            
            base_datatype = datatype.split('(')[0].strip().lower()
            if base_datatype in ['bigint', 'integer', 'int']:
                new_master_info['manual_num'].append(col)
            else:
                remark = "Column: {0} is NOT bigint or integer (data type = {1})".format(col, datatype)
                self.logger.error("[{0}] {1}".format(self.name, remark))
                manual_num_err.append(remark)

        self.logger.info("DEBUG: new_master_info = {0}".format(new_master_info))
        return new_master_info, manual_num_err

    def run(self):
        while(True):
            if self.abort_event.is_set():
                break

            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            db = task['db']
            schema = task['schema']
            partition = task['partition']
            start_datetime = datetime.now()
            start_t = time.time()

            table = task['partition'].strip().lower()
            full_name = "{0}.{1}.{2}".format(db, schema, table)

            # pre-defined logging info
            self.db = db
            self.schema = schema
            self.short_name = "{0}.{1}".format(self.schema, table)
            self.start_time_tbl = start_t
            self.start_ts_tbl = datetime.fromtimestamp(start_t).strftime("%Y-%m-%d %H:%M:%S")
            #self.status = "PROCESSING"
            self.reconcile_method = ['count']

            base_table = partition.split('_1_prt_')[0] if '_1_prt_' in partition else partition

            try:
                self.tracker.update_worker_status(self.name, "[BUSY] {0}".format(partition))                
                
                # Step 1: Check Succeed Log
                log_row, log_msg = self.log_parser.get_latest_succeed_info(db, schema, partition)
                if not log_row:
                    raise ValueError(log_msg)
                
                local_file_path = log_row.get('File_Path')
                if self.config.replace_path_from and self.config.replace_path_to:
                    original_path = local_file_path
                    local_file_path = local_file_path.replace(self.config.replace_path_from, self.config.replace_path_to)
                    
                    if original_path != local_file_path:
                        self.logger.info("[Worker] Replaced local_file_path from '{0}' to '{1}'".format(original_path, local_file_path))

                # Step 2: HDFS Sync
                hdfs_dest = os.path.join(self.config.hdfs_path, db, schema, partition)
                self.hdfs_h.sync_parquet(local_file_path, hdfs_dest)

                # Step 3: Fetch Metadata & Categorize
                type_map = self.meta_fetcher.fetch_data_types(db, base_table)
                missing_meta = True if type_map is None else False
                type_map = type_map or {}

                master_info = self.config.master_data.get((db, schema, base_table), {'manual_num': []})
                
                ### Check if user's manual input column exists in table ###
                manual_num_err = []
                new_master_info = {'manual_num': []}
                if not missing_meta: 
                    new_master_info, manual_num_err = self._check_manual_num(master_info, type_map)
                
                cat_cols = {'SUM_MIN_MAX': [], 'MIN_MAX': [], 'MD5_MIN_MAX': [], 'TYPE_MAP': type_map, 
                            'MANUAL_NUM': new_master_info['manual_num']}
                ### ======================================================================= ###
                thai_config = self.config.thai_dict.get((db.lower(), partition.lower()), {})
                if not thai_config:
                    thai_config = self.config.thai_dict.get((db.lower(), base_table.lower()), {})                
                
                if not missing_meta:
                    for col, gp_dt in type_map.items():
                        gp_base = gp_dt.split('(')[0].strip().lower()
                        thai_flag = thai_config.get(col)
                        if thai_flag == 'Y':
                            gp_base = 'thai_col_flag_y'
                        elif thai_flag == 'N':
                            gp_base = 'thai_col_flag_n'

                        if gp_base in self.config.type_mapping.get("SUM_MIN_MAX", []):
                            cat_cols['SUM_MIN_MAX'].append(col)
                            self.reconcile_method.append('number_sum_min_max')
                        elif gp_base in self.config.type_mapping.get("MIN_MAX", []):
                            cat_cols['MIN_MAX'].append(col)
                            self.reconcile_method.append('dttm_min_max')
                        elif gp_base in self.config.type_mapping.get("MD5_MIN_MAX", []):
                            cat_cols['MD5_MIN_MAX'].append(col)
                            self.reconcile_method.append('md5_min_max')

                # Step 4: Build & Execute SparkSQL Expressions
                self.spark.sparkContext.setJobGroup(partition, "Query: " + partition)
                df = self.spark.read.parquet(hdfs_dest)
                # FOR DEBUG
                # self.logger.info("Total rows in raw parquet (count action): {}".format(df.count()))
                # self.logger.info("Raw Parquet Schema:")
                # df.printSchema()
                agg_exprs = self.query_builder.build_agg_exprs(cat_cols)
                
                self.logger.info("Worker {0} executing Spark Action for {1}...".format(self.name, partition))
                # FOR DEBUG
                # failure_found = False
                # for expr in agg_exprs:
                #     try:
                #         self.logger.info("Testing expr: {}".format(expr))
                #         df.agg(expr).collect() 
                #     except Exception as inner_e:
                #         self.logger.error("FAULTY EXPRESSION FOUND: {} \nError: {}".format(expr, str(inner_e)))
                #         failure_found = True
                #         break

                # if failure_found:
                #     raise RuntimeError("Aborting due to bad SQL expression.")
                sp_row = df.agg(*agg_exprs).collect()[0]
                
                sp_res = sp_row.asDict()
                self.spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)

                def format_val(v):
                    if v is None: return None
                    v_str = str(v)
                    if 'E' in v_str or 'e' in v_str:
                        try:
                            return "{:f}".format(Decimal(v_str))
                        except Exception:
                            pass
                    return v
                
                final_json = collections.OrderedDict()
                final_json["table"] = "{0}.{1}.{2}".format(db, schema, partition)
                final_json["source_type"] = "parquet"
                final_json["count"] = int(sp_res.pop("count", 0))
                final_json["methods"] = collections.OrderedDict()
                
                parsed_res = {}
                for k, v in sp_res.items():
                    parts = k.split('|')
                    if len(parts) == 3:
                        method, col, func = parts
                        if method not in parsed_res: parsed_res[method] = {}
                        if col not in parsed_res[method]: parsed_res[method][col] = {}
                        parsed_res[method][col][func] = format_val(v)

                for method in ["SUM_MIN_MAX", "MIN_MAX", "MD5_MIN_MAX"]:
                    if method in parsed_res:
                        final_json["methods"][method] = collections.OrderedDict()
                        for col in sorted(parsed_res[method].keys()):
                            final_json["methods"][method][col] = collections.OrderedDict()
                            final_json["methods"][method][col]["data_type"] = type_map.get(col, "unknown")
                            for f in ["sum", "min", "max", "min_md5", "max_md5"]:
                                if f in parsed_res[method][col]:
                                    final_json["methods"][method][col][f] = parsed_res[method][col][f]

                # Step 5: Write to NAS
                query_file_name = "query_{0}_{1}_{2}_{3}.sql".format(db, schema, partition, self.global_ts)
                local_query_file = os.path.join(self.out_path, query_file_name)
                
                with open(local_query_file, 'w') as f:
                    f.write("-- PySpark Aggregation Expressions for {0}\n".format(partition))
                    for expr in agg_exprs:
                        f.write(str(expr) + "\n")

                out_file_name = "parquet_{0}_{1}_{2}_{3}.json".format(db, schema, partition, self.global_ts)
                self.local_json_file = os.path.join(self.out_path, out_file_name)
                
                with open(self.local_json_file, 'w') as f:
                    json.dump(final_json, f)

                copy_success, copy_err = self._copy_file_to_nas(self.local_json_file, db, schema, out_file_name)
                
                if copy_success:
                    self.logger.info("Worker {0} successfully saved local files and copied JSON to NAS for {1}".format(self.name, partition))
                else:
                    raise RuntimeError("Failed to copy file to NAS: {0}".format(copy_err))

                # Step 6: Finalize Status
                duration = time.time() - start_t
                status = "FAILED" if missing_meta else "SUCCESS"
                remark = "Metadata Missing (Count-only)" if missing_meta else "JSON Generated"

                if manual_num_err:
                    status = "FAILED"
                    remark = "{0} | {1}".format(remark, ",".join(manual_num_err))
                
                self.tracker.add_result(partition, status, duration, remark)
                #self.hive_logger.log_execution_status(self.execution_id, db, schema, base_table, partition, start_datetime, datetime.now(), duration, status.lower(), remark)

            except ValueError as ve:
                remark = str(ve)
                if "SKIPPED" in remark:
                    status = "SKIPPED"
                    remark = remark.replace("SKIPPED: ", "").replace("SKIPPED", "").strip()
                else:
                    status = "FAILED"
                self.tracker.add_result(partition, status, time.time() - start_t, remark)
                #self.hive_logger.log_execution_status(self.execution_id, db, schema, base_table, partition, start_datetime, datetime.now(), time.time() - start_t, status.lower(), msg)
            except Exception as e:
                remark = str(e)
                # self.logger.warning("Worker {0} failed on {1}: {2}".format(self.name, partition, repr(e)))
                self.logger.warning("Worker {0} failed on {1}: {2}".format(self.name, partition, e))
                self.tracker.add_result(partition, "FAILED", time.time() - start_t, "Error: {0}".format(remark[:50]))
                self.hive_logger.log_execution_status(self.execution_id, db, schema, base_table, partition, start_datetime, datetime.now(), time.time() - start_t, "failed", remark[:200])
                if "CRITICAL_FAILED:" in remark:
                    self.logger.critical("Worker {0} signaling global abort due to CRITICAL_FAILED condition.".format(self.name))
                    self.abort_event.set()
                    break
            finally:
                # After finishing processing, write one CSV line:
                try:
                    self.logger.info("Worker {0} logging status for {1}...".format(self.name, full_name))
                    self.logging_status(status, remark)
                except Exception as e:
                    self.logger.error("Failed writing logging_status for {}: {}".format(full_name, e))
                self.queue.task_done()

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
        lines.append(" RECONCILE PARQUET MONITOR (Python 2.7 / PySpark) ")
        lines.append("============================================================")
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
# 5. Main Job Class
# ==============================================================================

class ParquetQueryJob(object):
    def __init__(self, args, logger, log_path, global_date_folder, global_ts, main_path, final_out_dir, run_id):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.global_ts = global_ts
        self.run_id = run_id
        self.out_path = final_out_dir
        self.tracker = ProcessTracker(logger)
        self.global_date_folder = global_date_folder

        self.config = ConfigManager(args.env, args.master, args.map, args.list, args.table_name, logger, self.global_date_folder, self.run_id, self.global_ts, main_path)

        if not os.path.exists(self.config.succeed_path):
            self.logger.critical("Configured succeed_path does not exist: {0}".format(self.config.succeed_path))
            raise RuntimeError("Missing or invalid succeed_path directory.")

        self.logger.info("Initializing SparkSession with FAIR scheduler...")
        self.spark = SparkSession.builder.appName("script_reconcile_query_parquet") \
            .config("spark.scheduler.mode", "FAIR").enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.execution_id = self.spark.sparkContext.applicationId

        # Init Handlers
        self.log_parser = LogParser(self.config.succeed_path, logger)
        self.hdfs_h = HDFSHandler(self.spark, logger)
        self.meta_fetcher = MetadataFetcher(self.config.metadata_base_dir, logger)
        self.query_builder = SparkQueryBuilder(self.config.env_params, self.config.type_mapping, logger)
        self.hive_logger = HiveLogger(self.spark, logger)
        self.file_h = FileHandler(logger)
        self.status_file_locks = {}
        self.status_file_locks_lock = threading.Lock()

        self.job_queue = Queue.Queue()
        self.abort_event = threading.Event()
        for task in self.config.execution_list: self.job_queue.put(task)
        self.tracker.set_total_task(len(self.config.execution_list) + len(self.config.invalid_tables))

        for invalid_node in self.config.invalid_tables:
            self.tracker.add_result(invalid_node['table'], "FAILED", 0.0, invalid_node['reason'])

    def run(self):
        #input_name = os.path.splitext(os.path.basename(self.args.list))[0]
        if self.args.list:
            input_name = os.path.splitext(os.path.basename(self.args.list))[0]
        else:
            # If --table_name is used instead of --list, use a generic name for the status file
            input_name = "list_table"

        status_file_filenm = "log_stat_rc_{0}_{1}.csv".format(input_name, self.global_ts)

        num_workers = int(self.args.concurrency)
        workers = []
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.log_parser, self.hdfs_h, self.meta_fetcher, 
                       self.query_builder, self.hive_logger, self.spark, self.tracker, self.logger, self.execution_id, self.global_ts, self.out_path, self.abort_event, self.run_id, status_file_filenm, self.status_file_locks, self.status_file_locks_lock, self.file_h)
            workers.append(w)
            w.start()

        monitor = MonitorThread(self.tracker, num_workers, self.log_path)
        monitor.start()

        try:
            while not self.job_queue.empty():
                if self.abort_event.is_set():
                    break
                time.sleep(1)
            
            if self.abort_event.is_set():
                raise RuntimeError("Script aborted due to critical worker failure (e.g. Missing config).")
            
            self.job_queue.join()
            for w in workers: w.join()
        except KeyboardInterrupt:
            sys.stdout.write("\n\n>>> ABORTING SCRIPT... <<<\n\n")
            sys.stdout.flush()
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

                    #/xx/xx/xx/mig_reconcile_query_gp_output/YYYYMMDD/{db}/{schema}}/stat_csv
                    dest_path = os.path.join(self.config.nas_destination, rel, 'stat_csv')
                    #self.logger.info("Copying {0} to NAS path: {1}".format(f, dest_path))
                    self.file_h.copy_to_nas(f, dest_path)

                self.logger.info("Copy Stat file = {0} file(s) to NAS successfully".format(len(files)))
                self.logger.info("")

            self.spark.stop()
            self.tracker.print_summary(self.log_path, self.config.nas_destination)

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Parquet JSON Query Builder')
    parser.add_argument('--env', default='env_config.txt')
    parser.add_argument('--master', help='Override config_master_file_path defined in env_config')
    parser.add_argument('--map', default='data_type_mapping.json')
    #parser.add_argument('--list', default='list_table.txt')
    #parser.add_argument('--table_name', help='Specific tables to run (DB|Schema.Partition)')
    parser.add_argument('--concurrency', default=4, type=int)

    # handle table and list as mutually exclusive arguments
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list', help='Path to list file (e.g. list_table.txt)')
    group.add_argument('--table_name', help='Specific tables to run (DB|Schema.Partition)')

    # Parse arguments
    args = parser.parse_args()

    # Set default list file if neither --list nor --table_name is provided
    if args.list is None and args.table_name is None:
        args.list = 'list_table.txt'

    def resolve_config_path(input_path, base_dir):
        if not input_path: return input_path
        if os.path.isabs(input_path):
            return input_path
        return os.path.join(base_dir, 'config', input_path)

    # Resolve paths
    args.env = resolve_config_path(args.env, main_path)
    if args.master is not None:
        args.master = resolve_config_path(args.master, main_path)
    args.map = resolve_config_path(args.map, main_path)

    #args.list = resolve_config_path(args.list, main_path)
    if args.list is not None:
        args.list = resolve_config_path(args.list, main_path)

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")
    run_id = str(uuid.uuid4().hex)

    final_log_dir = os.path.join(main_path, 'log', global_date_folder)
    final_out_dir = os.path.join(main_path, 'output', global_date_folder)

    if not os.path.exists(final_out_dir):
        try:
            os.makedirs(final_out_dir)
        except OSError as e:
            print("WARNING: Could not create output directory '{0}'. Using current directory. Error: {1}".format(final_out_dir, e))
    
    logger, log_path = setup_logging(final_log_dir, 'reconcile_query_parquet', global_ts)
    logger.info("Started Reconcile script with concurrency: {0}".format(args.concurrency))
    logger.info("Run ID: {0}".format(run_id))

    try:
        job = ParquetQueryJob(args, logger, log_path, global_date_folder, global_ts, main_path, final_out_dir, run_id)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e), exc_info=True)
        sys.exit(1)