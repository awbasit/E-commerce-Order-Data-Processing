"""
Data aggregation functions
"""
from typing import Dict, Any
from collections import defaultdict


class DataAggregator:
    """Aggregate data across multiple chunks"""
    
    def merge_category_kpis(self, existing_kpis: Dict[str, Any], new_kpis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge category KPIs from multiple chunks
        
        Args:
            existing_kpis: Existing KPI data
            new_kpis: New KPI data to merge
            
        Returns:
            Merged KPI data
        """
        merged = existing_kpis.copy()
        
        for key, new_data in new_kpis.items():
            if key in merged:
                # Merge with existing data
                existing_data = merged[key]
                
                # Aggregate numerical values
                merged[key] = {
                    'category': new_data['category'],
                    'order_date': new_data['order_date'],
                    'total_orders': existing_data['total_orders'] + new_data['total_orders'],
                    'daily_revenue': existing_data['daily_revenue'] + new_data['daily_revenue'],
                    'total_items_sold': existing_data['total_items_sold'] + new_data['total_items_sold'],
                    'unique_customers': existing_data['unique_customers'] + new_data['unique_customers'],
                    'timestamp': new_data['timestamp']
                }
                
                # Recalculate averages
                total_orders = merged[key]['total_orders']
                if total_orders > 0:
                    merged[key]['avg_order_value'] = merged[key]['daily_revenue'] / total_orders
                    merged[key]['avg_items_per_order'] = merged[key]['total_items_sold'] / total_orders
                
                # Recalculate return rate (simplified - would need more complex logic in production)
                merged[key]['avg_return_rate'] = (
                    existing_data['avg_return_rate'] * existing_data['total_orders'] +
                    new_data['avg_return_rate'] * new_data['total_orders']
                ) / total_orders
                
            else:
                # Add new data
                merged[key] = new_data
        
        return merged
    
    def merge_daily_kpis(self, existing_kpis: Dict[str, Any], new_kpis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge daily KPIs from multiple chunks
        
        Args:
            existing_kpis: Existing KPI data
            new_kpis: New KPI data to merge
            
        Returns:
            Merged KPI data
        """
        merged = existing_kpis.copy()
        
        for date_key, new_data in new_kpis.items():
            if date_key in merged:
                # Merge with existing data
                existing_data = merged[date_key]
                
                # Aggregate numerical values
                merged[date_key] = {
                    'order_date': new_data['order_date'],
                    'total_orders': existing_data['total_orders'] + new_data['total_orders'],
                    'total_revenue': existing_data['total_revenue'] + new_data['total_revenue'],
                    'total_items_sold': existing_data['total_items_sold'] + new_data['total_items_sold'],
                    'unique_customers': existing_data['unique_customers'] + new_data['unique_customers'],
                    'timestamp': new_data['timestamp']
                }
                
                # Recalculate averages
                total_orders = merged[date_key]['total_orders']
                if total_orders > 0:
                    merged[date_key]['avg_order_value'] = merged[date_key]['total_revenue'] / total_orders
                    merged[date_key]['avg_items_per_order'] = merged[date_key]['total_items_sold'] / total_orders
                
                # Recalculate return rate
                merged[date_key]['return_rate'] = (
                    existing_data['return_rate'] * existing_data['total_orders'] +
                    new_data['return_rate'] * new_data['total_orders']
                ) / total_orders
                
                # Merge category breakdown
                merged[date_key]['category_breakdown'] = self._merge_category_breakdown(
                    existing_data.get('category_breakdown', {}),
                    new_data.get('category_breakdown', {})
                )
                
            else:
                # Add new data
                merged[date_key] = new_data
        
        return merged
    
    def _merge_category_breakdown(self, existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """Merge category breakdown data"""
        merged = existing.copy()
        
        for category, data in new.items():
            if category in merged:
                # Merge data for existing category
                for metric, value in data.items():
                    merged[category][metric] = merged[category].get(metric, 0) + value
            else:
                # Add new category
                merged[category] = data
        
        return merged
