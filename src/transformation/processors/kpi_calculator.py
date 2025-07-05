"""
KPI calculation functions
"""
import pandas as pd
from typing import Dict, Any
from datetime import datetime


class KPICalculator:
    """Calculate business KPIs from order data"""
    
    def calculate_category_kpis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate category-level KPIs
        
        Args:
            df: DataFrame with order data
            
        Returns:
            Dict with category KPIs
        """
        # Convert data types
        df = self._prepare_dataframe(df)
        
        # Group by category and order_date
        category_kpis = {}
        
        for (category, order_date), group in df.groupby(['category', 'order_date']):
            # Calculate KPIs for this category-date combination
            total_orders = len(group)
            total_revenue = group['total_amount'].sum()
            avg_order_value = group['total_amount'].mean()
            
            # Calculate return rate
            returned_orders = len(group[group['status'] == 'returned'])
            return_rate = returned_orders / total_orders if total_orders > 0 else 0
            
            # Calculate additional metrics
            total_items = group['quantity'].sum()
            unique_customers = group['customer_id'].nunique()
            avg_items_per_order = group['quantity'].mean()
            
            key = f"{category}#{order_date}"
            category_kpis[key] = {
                'category': category,
                'order_date': str(order_date),
                'total_orders': int(total_orders),
                'daily_revenue': float(total_revenue),
                'avg_order_value': float(avg_order_value),
                'avg_return_rate': float(return_rate),
                'total_items_sold': int(total_items),
                'unique_customers': int(unique_customers),
                'avg_items_per_order': float(avg_items_per_order),
                'timestamp': datetime.now().isoformat()
            }
        
        return category_kpis
    
    def calculate_daily_kpis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate daily aggregate KPIs
        
        Args:
            df: DataFrame with order data
            
        Returns:
            Dict with daily KPIs
        """
        # Convert data types
        df = self._prepare_dataframe(df)
        
        # Group by order_date
        daily_kpis = {}
        
        for order_date, group in df.groupby('order_date'):
            # Calculate daily KPIs
            total_orders = len(group)
            total_revenue = group['total_amount'].sum()
            total_items_sold = group['quantity'].sum()
            unique_customers = group['customer_id'].nunique()
            
            # Calculate return rate
            returned_orders = len(group[group['status'] == 'returned'])
            return_rate = returned_orders / total_orders if total_orders > 0 else 0
            
            # Calculate additional metrics
            avg_order_value = group['total_amount'].mean()
            avg_items_per_order = group['quantity'].mean()
            
            # Category breakdown
            category_breakdown = group.groupby('category').agg({
                'total_amount': 'sum',
                'order_id': 'count'
            }).to_dict()
            
            daily_kpis[str(order_date)] = {
                'order_date': str(order_date),
                'total_orders': int(total_orders),
                'total_revenue': float(total_revenue),
                'total_items_sold': int(total_items_sold),
                'return_rate': float(return_rate),
                'unique_customers': int(unique_customers),
                'avg_order_value': float(avg_order_value),
                'avg_items_per_order': float(avg_items_per_order),
                'category_breakdown': category_breakdown,
                'timestamp': datetime.now().isoformat()
            }
        
        return daily_kpis
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for KPI calculation"""
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Convert data types
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        # Remove rows with invalid data
        df = df.dropna(subset=['quantity', 'unit_price', 'total_amount', 'order_date'])
        
        # Convert order_date to date only (remove time component)
        df['order_date'] = df['order_date'].dt.date
        
        return df