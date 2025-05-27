import chardet
import pandas as pd

def detect_non_utf8(file_path):
    # First detect the file encoding
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        encoding = chardet.detect(raw_data)['encoding']
    
    print(f"Detected encoding: {encoding}")
    
    # Try to find problematic lines
    with open(file_path, 'rb') as f:
        for i, line in enumerate(f, 1):
            try:
                line.decode('utf-8')
            except UnicodeDecodeError as e:
                print(f"\nNon-UTF-8 character found on line {i}:")
                print(f"Error: {e}")
                print(f"Problematic bytes: {line[e.start:e.end]}")
                print(f"Full line (hex): {line.hex()}")
    
    # Try to read with detected encoding
    try:
        df = pd.read_csv(file_path, encoding=encoding, sep=";")
        print("\nFile read successfully with encoding:", encoding)
        return df
    except Exception as e:
        print("\nFailed to read with detected encoding:", e)
        return None

# Usage
file_path = r"C:\Users\dell\Airflow_Docker\ventes.csv"
df = detect_non_utf8(file_path)
if df is not None:
    print(df.head())