import pandas as pd
import json


# Sample data
data = [
    '''{"resource": {"telemetry_sdk_language": "python", "telemetry_sdk_name": "opentelemetry", "resourceSpans": {"attribute": "telemetry", "scopeSpans": "randomResource"}}}''',
    '''{"resource": {"telemetry_sdk_language": "java", "telemetry_sdk_name": "opentelemetry", "resourceSpans": {"attribute": "telemetry1", "scopeSpans": "randomResource2"}}}'''
]

# Create initial DataFrame
df = pd.DataFrame(data, columns=['json_str'])

# Function to convert string enclosed JSON to dictionary
def json_string_to_dict(json_str):
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

# Apply function to convert JSON strings to dictionaries
df['json_dict'] = df['json_str'].apply(json_string_to_dict)

# Drop rows where JSON conversion failed
df = df.dropna(subset=['json_dict'])

# Normalize JSON dictionaries into a flat DataFrame
normalized_df = json_normalize(df['json_dict'])

# Show the resulting DataFrame
print(normalized_df)
