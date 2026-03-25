import sys
import json
import os
import uuid
import pandas as pd

def main():
    if len(sys.argv) != 4:
        print("Usage: python parquet_writer.py <json_payload_path> <status_dir> <result_dir>")
        sys.exit(1)

    json_path = sys.argv[1]
    status_dir = sys.argv[2]
    result_dir = sys.argv[3]

    try:
        with open(json_path, 'r') as f:
            payload = json.load(f)

        file_uuid = str(uuid.uuid4())

        df_status = pd.DataFrame([payload['status']])
        df_status.to_parquet(os.path.join(status_dir, "part_{0}.parquet".format(file_uuid)), engine='pyarrow', index=False)

        if payload['result']:
            df_result = pd.DataFrame(payload['result'])
            df_result.to_parquet(os.path.join(result_dir, "part_{0}.parquet".format(file_uuid)), engine='pyarrow', index=False)

        print(file_uuid)
    except Exception as e:
        print("ERROR: {0}".format(e))
        sys.exit(1)

if __name__ == '__main__':
    main()