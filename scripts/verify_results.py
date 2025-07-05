"""
Script to verify pipeline results in DynamoDB
"""
import boto3
import os
from datetime import datetime, timedelta

def verify_results(environment):
    """Verify that data was processed and stored correctly"""
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Table names
    category_table = dynamodb.Table(f'CategoryDailyKPIs-{environment}')
    daily_table = dynamodb.Table(f'DailyOrderKPIs-{environment}')
    
    print(f"🔍 Verifying results in {environment} environment...")
    
    # Check category KPIs
    try:
        response = category_table.scan()
        category_count = response['Count']
        print(f"✅ Category KPIs found: {category_count} records")
        
        if category_count > 0:
            sample_item = response['Items'][0]
            print(f"   Sample: {sample_item['category']} on {sample_item['order_date']} - ${sample_item['daily_revenue']}")
        
    except Exception as e:
        print(f"❌ Error checking category KPIs: {e}")
        return False
    
    # Check daily KPIs
    try:
        response = daily_table.scan()
        daily_count = response['Count']
        print(f"✅ Daily KPIs found: {daily_count} records")
        
        if daily_count > 0:
            sample_item = response['Items'][0]
            print(f"   Sample: {sample_item['order_date']} - {sample_item['total_orders']} orders, ${sample_item['total_revenue']}")
        
    except Exception as e:
        print(f"❌ Error checking daily KPIs: {e}")
        return False
    
    if category_count > 0 and daily_count > 0:
        print("✅ Pipeline verification successful!")
        return True
    else:
        print("❌ No data found - pipeline may have failed")
        return False

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Verify pipeline results')
    parser.add_argument('--environment', required=True, choices=['dev', 'staging', 'prod'])
    
    args = parser.parse_args()
    
    success = verify_results(args.environment)
    exit(0 if success else 1)

if __name__ == "__main__":
    main()