import json
import yaml


# Function to convert JSON to YAML
def json_to_yaml(input_json_file, output_yaml_file):
    # Read the JSON file
    with open(input_json_file, 'r') as json_file:
        json_data = json.load(json_file)

    # Write to the YAML file
    with open(output_yaml_file, 'w') as yaml_file:
        yaml.dump(json_data, yaml_file, default_flow_style=False)


# Input JSON file path
input_json_file = '/Users/siva/Desktop/DWTS/dwts.json'

# Output YAML file path
output_yaml_file = '/Users/siva/Desktop/DWTS/output.yaml'

# Convert JSON to YAML
json_to_yaml(input_json_file, output_yaml_file)

print(f"Converted {input_json_file} to {output_yaml_file}")
