#!/usr/bin/env python3
"""
Импортирует исторические данные из Zabbix за указанный период
"""

import time
import logging
import requests
import urllib3
import json
import re
import os
import sys
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ZABBIX_CONFIG = {
    'url': "https://192.168.1.2",
    'user': "user",
    'password': "password",
    'timeout': 60
}

VICTORIAMETRICS_CONFIG = {
    'url': "http://localhost:8428",
    'import_path': "/api/v1/import",
    'timeout': 60
}

IMPORT_CONFIG = {
    'hostids': [host_id],
    'start_date': "2026-01-01 00:00:00",
    'end_date': "2026-02-01 00:00:00",
    'batch_size': 10000,
    'delay_between_batches': 0.1,
    'item_filter': None,
    'value_type': 'both',
    'log_level': "INFO"
}

class ZabbixHistoricalImporter:
    def __init__(self):
        self.setup_logging()
        self.auth_token = None
        self.session = requests.Session()
        self.stats = {
            'total_metrics': 0,
            'successful_metrics': 0,
            'failed_batches': 0,
            'start_time': time.time()
        }
        
    def setup_logging(self):
        log_level = getattr(logging, IMPORT_CONFIG['log_level'])
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('/var/log/zabbix_vm_historical_import.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger('zabbix_vm_historical_import')
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout))
    )
    def zabbix_api_call(self, method, params):
        api_url = f"{ZABBIX_CONFIG['url']}/api_jsonrpc.php"
        
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        
        if self.auth_token and method != "user.login":
            payload["auth"] = self.auth_token
        
        try:
            response = self.session.post(
                api_url,
                json=payload,
                timeout=ZABBIX_CONFIG['timeout'],
                verify=False
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'error' in result:
                    self.logger.error(f"Zabbix API error: {result['error']}")
                    return None
                return result.get('result')
            else:
                self.logger.error(f"HTTP error {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"API call error: {e}")
            raise
    
    def connect_zabbix(self):
        try:
            version = self.zabbix_api_call("apiinfo.version", {})
            if version:
                self.logger.info(f"Zabbix API version: {version}")
            
            auth_result = self.zabbix_api_call("user.login", {
                "user": ZABBIX_CONFIG['user'],
                "password": ZABBIX_CONFIG['password']
            })
            
            if auth_result:
                self.auth_token = auth_result
                self.logger.info("✅ Successfully authenticated to Zabbix API")
                return True
            else:
                self.logger.error("❌ Authentication failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    def get_items_for_hosts(self, hostids, filter_pattern=None):
        self.logger.info(f"📋 Getting items for hosts: {hostids}")
        
        params = {
            "output": ["itemid", "key_", "name", "value_type", "units", "hostid"],
            "hostids": hostids,
        }
        
        if filter_pattern:
            search_pattern = filter_pattern.replace('%', '')
            params["search"] = {"key_": search_pattern}
            params["searchWildcardsEnabled"] = True
        
        items = self.zabbix_api_call("item.get", params)
        
        if not items:
            self.logger.warning(f"No items found for hosts {hostids} with filter {filter_pattern}")
            return {
                'uint': [],
                'float': [],
                'all': []
            }
        
        self.logger.info(f"✅ Found {len(items)} items")
        
        uint_items = []
        float_items = []
        
        for item in items:
            value_type = item.get('value_type', '0')
            if value_type == '3':
                uint_items.append(item)
            elif value_type == '0':
                float_items.append(item)
        
        self.logger.info(f"   - Unsigned int items: {len(uint_items)}")
        self.logger.info(f"   - Float items: {len(float_items)}")
        
        return {
            'uint': uint_items,
            'float': float_items,
            'all': items
        }
    
    def get_history_data(self, itemids, start_time, end_time, value_type='uint'):
        if not itemids:
            return []
        
        history_table = 3 if value_type == 'uint' else 0
        all_history = []
        chunk_size = 100
        
        for i in range(0, len(itemids), chunk_size):
            chunk_ids = itemids[i:i+chunk_size]
            
            params = {
                "output": ["itemid", "clock", "value", "ns"],
                "history": history_table,
                "itemids": chunk_ids,
                "time_from": int(start_time),
                "time_till": int(end_time),
                "sortfield": "clock",
                "sortorder": "ASC",
                "limit": 50000
            }
            
            self.logger.debug(f"Requesting {value_type} history for {len(chunk_ids)} items")
            
            history = self.zabbix_api_call("history.get", params)
            
            if history:
                all_history.extend(history)
        
        self.logger.info(f"Total {value_type} history records: {len(all_history)}")
        return all_history
    
    def convert_to_vm_format(self, history_data, item_info):
        vm_metrics = []
        
        for record in history_data:
            itemid = str(record['itemid'])
            item = item_info.get(itemid, {})
            if not item:
                continue
            
            key = item.get('key_', 'unknown')
            
            timestamp = int(record['clock']) * 1000
            
            metric_name = self.create_safe_metric_name(key)
            
            labels = {
                'hostid': str(item.get('hostid', 'unknown')),
                'itemid': itemid,
                'item_key': self.clean_label_value(key),
                'item_name': self.clean_label_value(item.get('name', ''))
            }
            
            if 'value' in record:
                try:
                    value = float(record['value'])
                except (ValueError, TypeError):
                    value = 0.0
                
                vm_metrics.append({
                    "metric": {
                        "__name__": metric_name,
                        **labels
                    },
                    "values": [value],
                    "timestamps": [timestamp]
                })
        
        return vm_metrics
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2)
    )
    def send_to_victoriametrics(self, metrics_batch):
        if not metrics_batch:
            return True
        
        vm_url = f"{VICTORIAMETRICS_CONFIG['url']}{VICTORIAMETRICS_CONFIG['import_path']}"
        
        try:
            lines = []
            for metric in metrics_batch:
                lines.append(json.dumps(metric, ensure_ascii=False, separators=(',', ':')))
            
            data_to_send = '\n'.join(lines)
            timestamp = int(time.time())
            filename = f"/tmp/vm_data_{timestamp}.json"
            with open(filename, 'w') as f:
                f.write(data_to_send[:5000])
            self.logger.info(f"💾 Saved sample data to {filename}")
            
            response = self.session.post(
                vm_url,
                data=data_to_send,
                headers={'Content-Type': 'application/json'},
                timeout=VICTORIAMETRICS_CONFIG['timeout']
            )
            
            if response.status_code == 204:
                return True
            else:
                self.logger.error(f"HTTP {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending to VM: {e}")
            raise
    
    def create_safe_metric_name(self, key):
        if not key:
            return "zabbix_unknown"
        
        safe_key = key.replace('.', '_').replace('[', '_').replace(']', '_')
        safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', safe_key)
        safe_key = re.sub(r'_+', '_', safe_key)
        safe_key = safe_key.strip('_')
        
        return f"zabbix_{safe_key}"
    
    def clean_label_value(self, value):
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        
        cleaned = re.sub(r'[^\x20-\x7E]', '', value)
        return cleaned[:500]
    
    def split_time_range(self, start_date, end_date, days_per_chunk=1):
        chunks = []
        current = start_date
        
        while current < end_date:
            chunk_end = min(current + timedelta(days=days_per_chunk), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end
        
        return chunks
    
    def parse_datetime(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.error(f"Неподдерживаемый формат даты: {date_str}")
                raise
    
    def import_historical_data(self):
        self.logger.info("🚀 STARTING HISTORICAL DATA IMPORT")
        
        try:
            start_date = self.parse_datetime(IMPORT_CONFIG['start_date'])
            end_date = self.parse_datetime(IMPORT_CONFIG['end_date'])
        except ValueError as e:
            self.logger.error(f"Date parsing error: {e}")
            return False
        
        self.logger.info(f"📅 Period: {start_date} to {end_date}")
        
        items = self.get_items_for_hosts(IMPORT_CONFIG['hostids'], IMPORT_CONFIG['item_filter'])
        
        if not items['all']:
            self.logger.error(f"No items found for hosts {IMPORT_CONFIG['hostids']}")
            return False
        
        item_info = {}
        for item in items['all']:
            item_info[str(item['itemid'])] = item
        
        time_chunks = []
        current = start_date
        while current < end_date:
            chunk_end = min(current + timedelta(days=1), end_date)
            time_chunks.append((current, chunk_end))
            current = chunk_end
        
        self.logger.info(f"📊 Split into {len(time_chunks)} time chunks")
        
        total_chunks = len(time_chunks)
        for chunk_idx, (chunk_start, chunk_end) in enumerate(time_chunks, 1):
            chunk_start_ts = int(chunk_start.timestamp())
            chunk_end_ts = int(chunk_end.timestamp())
            
            self.logger.info(f"🔄 Processing chunk {chunk_idx}/{total_chunks}: {chunk_start} to {chunk_end}")
            
            value_types_to_import = []
            if IMPORT_CONFIG['value_type'] in ['uint', 'both']:
                value_types_to_import.append(('uint', items['uint']))
            if IMPORT_CONFIG['value_type'] in ['float', 'both']:
                value_types_to_import.append(('float', items['float']))
            
            for value_type, item_list in value_types_to_import:
                if not item_list:
                    continue
                
                itemids = [item['itemid'] for item in item_list]
                self.import_history_chunk(itemids, chunk_start_ts, chunk_end_ts, value_type, item_info)
            
            time.sleep(1)
        
        elapsed = time.time() - self.stats['start_time']
        self.logger.info("=" * 50)
        self.logger.info("📊 IMPORT STATISTICS")
        self.logger.info(f"   Total metrics processed: {self.stats['total_metrics']}")
        self.logger.info(f"   Successfully imported: {self.stats['successful_metrics']}")
        self.logger.info(f"   Failed batches: {self.stats['failed_batches']}")
        self.logger.info(f"   Time elapsed: {elapsed:.1f} seconds")
        self.logger.info("=" * 50)
        
        return True
    
    def import_history_chunk(self, itemids, start_ts, end_ts, value_type, item_info):
        if not itemids:
            return
        
        history = self.get_history_data(itemids, start_ts, end_ts, value_type)
        
        if not history:
            return
        
        self.stats['total_metrics'] += len(history)
        self.logger.info(f"   Got {len(history)} {value_type} records")
        
        vm_metrics = self.convert_to_vm_format(history, item_info)
        
        if not vm_metrics:
            return
        
        batch_size = IMPORT_CONFIG['batch_size']
        total_batches = (len(vm_metrics) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(vm_metrics))
            batch = vm_metrics[start_idx:end_idx]
            
            if self.send_to_victoriametrics(batch):
                self.stats['successful_metrics'] += len(batch)
                self.logger.info(f"      ✅ Sent batch {batch_idx + 1}/{total_batches}: {len(batch)} metrics")
            else:
                self.stats['failed_batches'] += 1
                self.logger.error(f"      ❌ Failed to send batch {batch_idx + 1}")
            
            time.sleep(IMPORT_CONFIG['delay_between_batches'])

def main():
    print("=" * 60)
    print("Zabbix to VictoriaMetrics Historical Data Importer")
    print("=" * 60)
    print(f"Start date: {IMPORT_CONFIG['start_date']}")
    print(f"End date: {IMPORT_CONFIG['end_date']}")
    print(f"Hosts: {IMPORT_CONFIG['hostids']}")
    print(f"Item filter: {IMPORT_CONFIG['item_filter']}")
    print("=" * 60)
    
    confirm = input("Continue with import? (y/N): ")
    if confirm.lower() != 'y':
        print("Import cancelled")
        return
    
    importer = ZabbixHistoricalImporter()
    
    if not importer.connect_zabbix():
        print("❌ Failed to connect to Zabbix")
        return
    
    success = importer.import_historical_data()
    
    if success:
        print("\n✅ Import completed successfully!")
    else:
        print("\n❌ Import failed")

if __name__ == '__main__':
    main()
