#!/usr/bin/env python3
"""
AWS API Gateway Unitary Cost Calculator
Calculate cost per API call and cost per GB of data transferred
"""

# Given cost data (monthly)
api_gateway_cost = 1311402.78  # USD
cloudwatch_logs_cost = 323905.24  # USD
total_cost = api_gateway_cost + cloudwatch_logs_cost

# Example usage metrics (monthly)
total_api_calls = 1000000  # 1 million API calls
total_data_transferred_gb = 500  # 500 GB

print("=== AWS API Gateway Cost Analysis ===")
print(f"Region: sa-east-1 (São Paulo)")
print(f"Period: Monthly")
print()

print("=== Cost Breakdown ===")
print(f"API Gateway Cost: ${api_gateway_cost:,.2f}")
print(f"CloudWatch Logs Cost: ${cloudwatch_logs_cost:,.2f}")
print(f"Total Cost: ${total_cost:,.2f}")
print()

print("=== Usage Metrics (Example) ===")
print(f"Total API Calls: {total_api_calls:,}")
print(f"Total Data Transferred Out: {total_data_transferred_gb:,} GB")
print()

print("=== Unitary Cost Calculations ===")

# Calculate cost per API call
cost_per_api_call = total_cost / total_api_calls
print(f"Cost per API Call: ${cost_per_api_call:.6f}")
print(f"Cost per 1,000 API Calls: ${cost_per_api_call * 1000:.4f}")

# Calculate cost per GB of data transferred
cost_per_gb = total_cost / total_data_transferred_gb
print(f"Cost per GB of Data Transferred: ${cost_per_gb:.2f}")

print()
print("=== Component-wise Unitary Costs ===")

# API Gateway component costs
api_gateway_cost_per_call = api_gateway_cost / total_api_calls
api_gateway_cost_per_gb = api_gateway_cost / total_data_transferred_gb

print(f"API Gateway Cost per API Call: ${api_gateway_cost_per_call:.6f}")
print(f"API Gateway Cost per GB: ${api_gateway_cost_per_gb:.2f}")

# CloudWatch Logs component costs
cloudwatch_cost_per_call = cloudwatch_logs_cost / total_api_calls
cloudwatch_cost_per_gb = cloudwatch_logs_cost / total_data_transferred_gb

print(f"CloudWatch Logs Cost per API Call: ${cloudwatch_cost_per_call:.6f}")
print(f"CloudWatch Logs Cost per GB: ${cloudwatch_cost_per_gb:.2f}")

print()
print("=== Cost Distribution ===")
api_gateway_percentage = (api_gateway_cost / total_cost) * 100
cloudwatch_percentage = (cloudwatch_logs_cost / total_cost) * 100

print(f"API Gateway: {api_gateway_percentage:.1f}% of total cost")
print(f"CloudWatch Logs: {cloudwatch_percentage:.1f}% of total cost")

