"""
Vehicle Tracking and Service Analytics Dashboard
Provides detailed tracking of vehicles by service period (daily, weekly, monthly)
with analytics, charts, and detailed invoice/order information.
"""

import logging
import json
from collections import defaultdict
import re
from datetime import datetime, timedelta
from decimal import Decimal
from django.db.models import Count, Sum, Q, F, DecimalField
from django.db.models.functions import Cast
from django.http import JsonResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_http_methods
from django.utils import timezone

from tracker.models import Vehicle, Order, Invoice, InvoiceLineItem, LabourCode, Customer
from tracker.utils.order_type_detector import _normalize_category_to_order_type
from .utils import get_user_branch

logger = logging.getLogger(__name__)


@login_required
def vehicle_tracking_dashboard(request):
    """
    Vehicle Tracking Dashboard - Shows vehicles that came for service
    with daily, weekly, and monthly analytics.
    """
    user_branch = get_user_branch(request.user)
    
    # Get filter parameters
    period = request.GET.get('period', 'monthly')  # daily, weekly, monthly
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    status_filter = request.GET.get('status', '')  # completed, pending, all
    order_type_filter = request.GET.get('order_type', '')  # service, sales, labour
    
    # Set default date range
    if not end_date:
        end_date = timezone.now().date()
    else:
        try:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        except:
            end_date = timezone.now().date()
    
    if not start_date:
        if period == 'daily':
            start_date = end_date
        elif period == 'weekly':
            start_date = end_date - timedelta(days=7)
        else:  # monthly
            start_date = end_date - timedelta(days=30)
    else:
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        except:
            start_date = end_date - timedelta(days=30)
    
    context = {
        'period': period,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'status_filter': status_filter,
        'order_type_filter': order_type_filter,
    }
    
    return render(request, 'tracker/vehicle_tracking_dashboard.html', context)


@login_required
@require_http_methods(["GET"])
def api_vehicle_tracking_data(request):
    """
    API endpoint for vehicle tracking data with filtering and aggregation.
    
    Query parameters:
    - period: daily|weekly|monthly
    - start_date: YYYY-MM-DD
    - end_date: YYYY-MM-DD
    - status: completed|pending|in_progress|all
    - order_type: service|sales|labour|mixed|all
    - search: search by plate number or customer name
    """
    user_branch = get_user_branch(request.user)
    
    try:
        period = request.GET.get('period', 'monthly')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        status_filter = request.GET.get('status', 'all')
        order_type_filter = request.GET.get('order_type', 'all')
        search_query = request.GET.get('search', '').strip()

        # Filter out 'undefined' from JavaScript (when no search is entered)
        if search_query == 'undefined' or search_query == 'null':
            search_query = ''

        # Parse dates
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else timezone.now().date()
        except:
            end_date = timezone.now().date()

        try:
            if start_date_str:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            else:
                start_date = end_date - timedelta(days=30)
        except:
            start_date = end_date - timedelta(days=30)

        logger.info(f"Vehicle tracking query - Period: {period}, Date range: {start_date} to {end_date}, Search: '{search_query}'")

        # Query vehicles that came for service based on invoices:
        # The vehicle is identified by the plate number (reference field) from the invoice
        # We track all vehicles with invoices in the date range, regardless of order type
        # (invoices can be associated with orders of any type: service, sales, labour, mixed, etc.)
        # Also include vehicles with orders in the date range for completeness
        invoices_query = Invoice.objects.filter(
            invoice_date__range=[start_date, end_date]
        )

        orders_query = Order.objects.filter(
            created_at__date__range=[start_date, end_date]
        )

        if user_branch:
            invoices_query = invoices_query.filter(branch=user_branch)
            orders_query = orders_query.filter(branch=user_branch)

        # Get vehicles from invoices
        vehicles_query = Vehicle.objects.filter(
            invoices__in=invoices_query
        ).distinct()

        # Also get vehicles from orders
        vehicles_from_orders = Vehicle.objects.filter(
            orders__in=orders_query
        ).distinct()

        # Combine both querysets
        vehicles_query = vehicles_query | vehicles_from_orders

        logger.info(f"Vehicles found before search filter: {vehicles_query.count()}")

        # Apply search filter
        if search_query:
            vehicles_query = vehicles_query.filter(
                Q(plate_number__icontains=search_query) |
                Q(customer__full_name__icontains=search_query)
            )
            logger.info(f"Vehicles found after search filter: {vehicles_query.count()}")
        
        vehicle_data = []

        logger.info(f"Processing {vehicles_query.count()} vehicles from query")

        def _plate_from_reference(ref: str):
            if not ref:
                return None
            s = str(ref).strip().upper()
            if s.startswith('FOR '):
                s = s[4:].strip()
            elif s.startswith('FOR'):
                s = s[3:].strip()
            if re.match(r'^[A-Z]{1,3}\s*-?\s*\d{1,4}[A-Z]?$', s) or \
               re.match(r'^[A-Z]{1,3}\d{3,4}$', s) or \
               re.match(r'^\d{1,4}[A-Z]{2,3}$', s) or \
               re.match(r'^[A-Z]\s*\d{1,4}\s*[A-Z]{2,3}$', s):
                return s.replace('-', '').replace(' ', '')
            return None

        for vehicle in vehicles_query:
            inv_qs = vehicle.invoices.filter(
                invoice_date__range=[start_date, end_date]
            )
            if user_branch:
                inv_qs = inv_qs.filter(branch=user_branch)
            # Include invoices that have either:
            # 1. A valid plate number in the reference field (extracted from invoice), OR
            # 2. A vehicle field directly set (linked during invoice upload)
            # This ensures vehicles are tracked even if the reference field doesn't contain a plate
            filtered_invoices = [inv for inv in inv_qs if _plate_from_reference(inv.reference) or inv.vehicle_id]
            if not filtered_invoices:
                continue

            # Get all orders for this vehicle in the date range (any type: service, sales, labour, mixed, inquiry)
            orders = vehicle.orders.filter(
                created_at__date__range=[start_date, end_date]
            )

            if user_branch:
                orders = orders.filter(branch=user_branch)

            # Also get orders linked through invoices in the same date range
            # This captures orders that are associated with invoices (the primary way to track vehicles)
            order_links_via_invoices = Order.objects.filter(
                invoices__vehicle=vehicle,
                invoices__invoice_date__range=[start_date, end_date]
            ).distinct()

            if user_branch:
                order_links_via_invoices = order_links_via_invoices.filter(branch=user_branch)

            # Calculate order statistics BEFORE union (Django doesn't support filter after union)
            def _count_by_status(qs):
                return {
                    'completed': qs.filter(status='completed').count(),
                    'in_progress': qs.filter(status='in_progress').count(),
                    'pending': qs.filter(status='created').count(),
                    'overdue': qs.filter(status='overdue').count(),
                    'cancelled': qs.filter(status='cancelled').count(),
                }

            # Get stats from both sources and combine them
            orders_stats = _count_by_status(orders)
            invoice_links_stats = _count_by_status(order_links_via_invoices)

            # Combine stats from both sources (sum the counts)
            order_stats = {
                'completed': orders_stats['completed'] + invoice_links_stats['completed'],
                'in_progress': orders_stats['in_progress'] + invoice_links_stats['in_progress'],
                'pending': orders_stats['pending'] + invoice_links_stats['pending'],
                'overdue': orders_stats['overdue'] + invoice_links_stats['overdue'],
                'cancelled': orders_stats['cancelled'] + invoice_links_stats['cancelled'],
            }

            # Combine orders from both sources for iteration
            all_orders = orders.union(order_links_via_invoices).order_by('-created_at')

            if not filtered_invoices and not all_orders.exists():
                continue

            # Calculate vehicle metrics
            total_spent = sum((inv.total_amount or Decimal('0')) for inv in filtered_invoices) if filtered_invoices else Decimal('0')
            invoice_count = len(filtered_invoices)

            # Get order types and categories from all orders
            # Note: vehicles are identified by plate number from invoice reference field,
            # regardless of order type. Orders can be service, sales, labour, mixed, or inquiry.
            order_types = set()
            service_types = set()

            for order in all_orders:
                order_types.add(order.type)

                # Extract categories from order's mixed_categories field
                # This captures the actual service/labour categories detected from invoice items
                if order.mixed_categories:
                    try:
                        categories = json.loads(order.mixed_categories)
                        for cat in categories:
                            service_types.add(cat)
                    except:
                        pass
            
            # Get invoice data with line items
            invoice_list = []
            for invoice in filtered_invoices:
                line_items = InvoiceLineItem.objects.filter(invoice=invoice)

                # Get categories for line items
                categories = set()
                line_items_data = []

                for item in line_items:
                    # Try to find labor code for this item
                    category = 'Service'
                    labour_code = None

                    if item.code:
                        labour_code = LabourCode.objects.filter(code__iexact=item.code).first()
                        if labour_code:
                            category = labour_code.category
                            categories.add(category)

                    line_items_data.append({
                        'code': item.code or '',
                        'description': item.description,
                        'qty': float(item.quantity),
                        'unit_price': float(item.unit_price),
                        'total': float(item.line_total),
                        'category': category,
                        'tax_rate': float(item.tax_rate) if item.tax_rate else 0,
                        'tax_amount': float(item.tax_amount) if item.tax_amount else 0,
                    })

                invoice_dict = {
                    'invoice_number': invoice.invoice_number,
                    'invoice_date': invoice.invoice_date.isoformat(),
                    'total_amount': float(invoice.total_amount),
                    'subtotal': float(invoice.subtotal),
                    'tax_amount': float(invoice.tax_amount),
                    'reference': invoice.reference or '',
                    'status': invoice.status,
                    'order_id': invoice.order_id,
                    'order_number': invoice.order.order_number if invoice.order else '',
                    'line_items_count': line_items.count(),
                    'categories': sorted(list(categories)) if categories else ['Service'],
                    'line_items': line_items_data
                }
                invoice_list.append(invoice_dict)
            
            # Apply status filter
            if status_filter != 'all':
                if status_filter == 'completed' and order_stats['completed'] == 0:
                    continue
                elif status_filter == 'pending' and order_stats.get('pending', 0) == 0:
                    continue
            
            # Do not exclude vehicles by order type; vehicles are identified by plate from invoice reference
            
            # Determine if returning vehicle (multiple visits/invoices)
            is_returning = invoice_count > 1

            # Prefer plate from: 1) most recent invoice reference, 2) vehicle's plate_number as fallback
            recent_plate = None
            try:
                if filtered_invoices:
                    try:
                        recent_invoice = max(
                            filtered_invoices,
                            key=lambda inv: inv.invoice_date or datetime.min
                        )
                    except Exception:
                        recent_invoice = filtered_invoices[0]
                    # Try to extract plate from reference field first
                    recent_plate = _plate_from_reference(recent_invoice.reference)
                    # If reference doesn't have a valid plate but invoice has vehicle, use vehicle's plate
                    if not recent_plate and recent_invoice.vehicle:
                        recent_plate = recent_invoice.vehicle.plate_number
            except Exception:
                recent_plate = None

            # Final fallback to vehicle's plate_number if not found from invoices
            if not recent_plate and vehicle:
                recent_plate = vehicle.plate_number

            vehicle_dict = {
                'id': vehicle.id,
                'plate_number': recent_plate,
                'make': vehicle.make or '',
                'model': vehicle.model or '',
                'vehicle_type': vehicle.vehicle_type or '',
                'customer_id': vehicle.customer.id,
                'customer_name': vehicle.customer.full_name,
                'customer_phone': vehicle.customer.phone or '',
                'total_spent': float(total_spent),
                'invoice_count': invoice_count,
                'is_returning': is_returning,
                'order_stats': order_stats,
                'order_types': sorted(list(order_types)),
                'service_types': sorted(list(service_types)) if service_types else [],
                'invoices': invoice_list,
                'order_count': all_orders.count(),
            }
            
            vehicle_data.append(vehicle_dict)
        
        # Sort by total spent (descending)
        vehicle_data.sort(key=lambda x: x['total_spent'], reverse=True)

        logger.info(f"Final vehicle_data count: {len(vehicle_data)}")

        # Calculate summary statistics
        summary = {
            'total_vehicles': len(vehicle_data),
            'total_spent': sum(v['total_spent'] for v in vehicle_data),
            'total_invoices': sum(v['invoice_count'] for v in vehicle_data),
            'returning_vehicles': sum(1 for v in vehicle_data if v['is_returning']),
            'order_stats': {
                'completed': sum(v['order_stats']['completed'] for v in vehicle_data),
                'in_progress': sum(v['order_stats']['in_progress'] for v in vehicle_data),
                'pending': sum(v['order_stats']['pending'] for v in vehicle_data),
                'overdue': sum(v['order_stats']['overdue'] for v in vehicle_data),
            }
        }

        logger.info(f"Summary: {summary}")

        return JsonResponse({
            'success': True,
            'data': vehicle_data,
            'summary': summary,
            'filters': {
                'period': period,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'status': status_filter,
                'order_type': order_type_filter,
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching vehicle tracking data: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
@require_http_methods(["GET"])
def api_vehicle_analytics(request):
    """
    API endpoint for vehicle analytics and trends.
    
    Returns:
    - Daily/weekly/monthly trends
    - Spending by order type
    - Vehicle visit frequency
    - Average spending per vehicle
    """
    user_branch = get_user_branch(request.user)
    
    try:
        period = request.GET.get('period', 'monthly')
        start_date_str = request.GET.get('start_date')
        end_date_str = request.GET.get('end_date')
        
        # Parse dates
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else timezone.now().date()
        except:
            end_date = timezone.now().date()
        
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else (end_date - timedelta(days=30))
        except:
            start_date = end_date - timedelta(days=30)
        
        # Get invoices in date range
        invoices_qs = Invoice.objects.filter(
            invoice_date__range=[start_date, end_date]
        )

        if user_branch:
            invoices_qs = invoices_qs.filter(branch=user_branch)

        logger.info(f"Analytics - Invoices in range {start_date} to {end_date}: {invoices_qs.count()}")

        # Fetch all invoices without database-level date truncation (SQLite compatibility)
        invoices_with_dates = invoices_qs.values(
            'invoice_date',
            'total_amount',
            'vehicle'
        ).order_by('invoice_date')

        # Group data by period in Python
        trends_dict = defaultdict(lambda: {'total_amount': Decimal('0'), 'invoice_count': 0, 'vehicles': set()})

        for invoice in invoices_with_dates:
            # Get the date portion from invoice_date (handle datetime if needed)
            invoice_date_value = invoice['invoice_date']
            if hasattr(invoice_date_value, 'date'):
                # It's a datetime, convert to date
                invoice_date = invoice_date_value.date()
            else:
                # It's already a date
                invoice_date = invoice_date_value

            # Determine grouping key based on period
            if period == 'daily':
                period_key = invoice_date
            elif period == 'weekly':
                # Group by week (Monday of that week)
                period_key = invoice_date - timedelta(days=invoice_date.weekday())
            else:  # monthly
                # Group by first day of month
                period_key = invoice_date.replace(day=1)

            trends_dict[period_key]['total_amount'] += invoice['total_amount'] or Decimal('0')
            trends_dict[period_key]['invoice_count'] += 1
            if invoice['vehicle']:
                trends_dict[period_key]['vehicles'].add(invoice['vehicle'])

        # Convert to list and sort by date
        trends_data = [
            {
                'date': date.isoformat() if date else '',
                'total_amount': float(data['total_amount']),
                'invoice_count': data['invoice_count'],
                'vehicle_count': len(data['vehicles']),
            }
            for date, data in sorted(trends_dict.items())
        ]
        
        # Spending by order type
        spending_by_type = invoices_qs.filter(
            order__type__isnull=False
        ).values('order__type').annotate(
            total=Sum('total_amount'),
            count=Count('id')
        ).order_by('-total')
        
        spending_by_type_data = [
            {
                'type': item['order__type'],
                'total': float(item['total'] or 0),
                'count': item['count'],
                'average': float((item['total'] or 0) / item['count']) if item['count'] > 0 else 0,
            }
            for item in spending_by_type
        ]
        
        # Top vehicles by spending
        top_vehicles = Vehicle.objects.filter(
            invoices__invoice_date__range=[start_date, end_date]
        ).annotate(
            total_spent=Sum('invoices__total_amount'),
            invoice_count=Count('invoices', distinct=True)
        ).filter(
            total_spent__isnull=False
        )

        if user_branch:
            top_vehicles = top_vehicles.filter(
                invoices__branch=user_branch
            )

        top_vehicles = top_vehicles.order_by('-total_spent')[:10]
        
        top_vehicles_data = [
            {
                'plate_number': v.plate_number,
                'customer_name': v.customer.full_name,
                'total_spent': float(v.total_spent or 0),
                'invoice_count': v.invoice_count,
                'average_per_invoice': float((v.total_spent or 0) / v.invoice_count) if v.invoice_count > 0 else 0,
            }
            for v in top_vehicles
        ]
        
        return JsonResponse({
            'success': True,
            'trends': trends_data,
            'spending_by_type': spending_by_type_data,
            'top_vehicles': top_vehicles_data,
        })
        
    except Exception as e:
        logger.error(f"Error fetching vehicle analytics: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)
