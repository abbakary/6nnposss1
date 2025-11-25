"""
Data diagnostics and seeding for vehicle tracking.
Helps identify and fix issues with missing data in the dashboard.
"""

import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal

from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from tracker.models import Vehicle, Invoice, Customer, Order, Branch, InvoiceLineItem

logger = logging.getLogger(__name__)


@login_required
@require_http_methods(["GET"])
def api_vehicle_tracking_diagnostics(request):
    """
    Check the status of vehicle tracking data.
    Returns information about data in the system and recommendations.
    """
    try:
        # Check if user is admin or staff
        if not (request.user.is_superuser or request.user.is_staff):
            return JsonResponse({
                'success': False,
                'message': 'Only admins can access this endpoint'
            }, status=403)

        # Get counts
        customer_count = Customer.objects.count()
        vehicle_count = Vehicle.objects.count()
        invoice_count = Invoice.objects.count()
        order_count = Order.objects.count()
        branch_count = Branch.objects.count()

        # Check for recent data
        today = timezone.now().date()
        thirty_days_ago = today - timedelta(days=30)

        recent_invoices_count = Invoice.objects.filter(
            invoice_date__range=[thirty_days_ago, today]
        ).count()

        recent_vehicles_count = Vehicle.objects.filter(
            invoices__invoice_date__range=[thirty_days_ago, today]
        ).distinct().count()

        # Check for invoices with/without vehicles
        invoices_with_vehicles = Invoice.objects.filter(
            vehicle__isnull=False
        ).count()

        invoices_without_vehicles = Invoice.objects.filter(
            vehicle__isnull=True
        ).count()

        # Check for invoices with valid references
        all_invoices = Invoice.objects.all()
        invoices_with_plate_refs = 0
        for inv in all_invoices:
            if _extract_plate(inv.reference):
                invoices_with_plate_refs += 1

        # Status determination
        status = 'ok'
        issues = []

        if invoice_count == 0:
            status = 'warning'
            issues.append('No invoices in database')

        if recent_invoices_count == 0:
            status = 'warning'
            issues.append('No invoices in the last 30 days')

        if recent_vehicles_count == 0 and recent_invoices_count > 0:
            status = 'warning'
            issues.append('Invoices exist but no vehicles linked to recent invoices')

        if invoices_without_vehicles > 0 and invoices_with_plate_refs == 0:
            status = 'warning'
            issues.append('Invoices exist but have no vehicle links and no plate references')

        return JsonResponse({
            'success': True,
            'status': status,
            'counts': {
                'customers': customer_count,
                'vehicles': vehicle_count,
                'invoices': invoice_count,
                'orders': order_count,
                'branches': branch_count,
            },
            'recent_data': {
                'invoices_last_30_days': recent_invoices_count,
                'vehicles_with_recent_invoices': recent_vehicles_count,
            },
            'invoice_details': {
                'total_invoices': invoice_count,
                'invoices_with_vehicles': invoices_with_vehicles,
                'invoices_without_vehicles': invoices_without_vehicles,
                'invoices_with_plate_references': invoices_with_plate_refs,
            },
            'issues': issues,
            'recommendation': (
                'Create sample data' if status == 'warning' and invoice_count == 0
                else 'Dashboard should show data' if status == 'ok'
                else 'Check data consistency'
            )
        })

    except Exception as e:
        logger.error(f"Error in vehicle tracking diagnostics: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


@login_required
@require_http_methods(["POST"])
def api_seed_vehicle_tracking_data(request):
    """
    Create sample vehicle tracking data for testing/demonstration.
    Only available to superusers.
    """
    try:
        # Check if user is admin
        if not request.user.is_superuser:
            return JsonResponse({
                'success': False,
                'message': 'Only superadmins can seed data'
            }, status=403)

        # Create or get default branch
        branch, _ = Branch.objects.get_or_create(
            code='DEFAULT',
            defaults={
                'name': 'Default Branch',
                'region': 'Main',
                'is_active': True
            }
        )

        created_count = {
            'customers': 0,
            'vehicles': 0,
            'invoices': 0,
            'orders': 0
        }

        # Create sample customers
        customer_data = [
            {'name': 'Test Customer 1', 'phone': '+25570123456', 'email': 'test1@example.com'},
            {'name': 'Test Customer 2', 'phone': '+25570123457', 'email': 'test2@example.com'},
            {'name': 'Test Customer 3', 'phone': '+25570123458', 'email': 'test3@example.com'},
        ]

        customers = []
        for data in customer_data:
            customer, created = Customer.objects.get_or_create(
                phone=data['phone'],
                defaults={
                    'full_name': data['name'],
                    'email': data['email'],
                    'branch': branch,
                    'address': f"Test Address - {data['name']}"
                }
            )
            customers.append(customer)
            if created:
                created_count['customers'] += 1

        # Create sample vehicles
        vehicle_specs = [
            {'plate': 'TAA001', 'make': 'Toyota', 'model': 'Corolla', 'type': 'Sedan'},
            {'plate': 'UBB001', 'make': 'Nissan', 'model': 'Sunny', 'type': 'Sedan'},
            {'plate': 'ECC001', 'make': 'Mitsubishi', 'model': 'Lancer', 'type': 'Sedan'},
        ]

        vehicles = []
        for i, spec in enumerate(vehicle_specs):
            customer = customers[i % len(customers)]
            vehicle, created = Vehicle.objects.get_or_create(
                customer=customer,
                plate_number=spec['plate'],
                defaults={
                    'make': spec['make'],
                    'model': spec['model'],
                    'vehicle_type': spec['type']
                }
            )
            vehicles.append(vehicle)
            if created:
                created_count['vehicles'] += 1

        # Create sample invoices for the last 25 days
        today = timezone.now().date()
        for day_offset in range(5, 26, 5):  # Create invoices on different dates
            invoice_date = today - timedelta(days=day_offset)

            for vehicle in vehicles:
                invoice_number = f"INV-{vehicle.plate_number}-{invoice_date.isoformat()}"
                
                invoice, created = Invoice.objects.get_or_create(
                    invoice_number=invoice_number,
                    defaults={
                        'branch': branch,
                        'customer': vehicle.customer,
                        'vehicle': vehicle,
                        'invoice_date': invoice_date,
                        'reference': vehicle.plate_number,
                        'subtotal': Decimal('50000.00'),
                        'tax_amount': Decimal('5000.00'),
                        'tax_rate': Decimal('10.00'),
                        'total_amount': Decimal('55000.00'),
                        'status': 'issued'
                    }
                )

                if created:
                    # Create line items
                    InvoiceLineItem.objects.create(
                        invoice=invoice,
                        code='SVC001',
                        description=f'Service for {vehicle.make} {vehicle.model}',
                        item_type='service',
                        quantity=Decimal('1'),
                        unit='PCS',
                        unit_price=Decimal('50000.00'),
                        line_total=Decimal('50000.00'),
                        tax_rate=Decimal('10.00'),
                        tax_amount=Decimal('5000.00')
                    )

                    # Create corresponding order
                    order_data = {
                        'customer': vehicle.customer,
                        'vehicle': vehicle,
                        'branch': branch,
                        'type': 'service',
                        'status': 'completed',
                        'priority': 'medium',
                        'description': f'Service for {vehicle.plate_number}'
                    }

                    # Set created_at to match invoice date
                    order = Order.objects.create(**order_data)
                    order.created_at = timezone.make_aware(
                        datetime.combine(invoice_date, datetime.min.time())
                    )
                    order.save()

                    created_count['invoices'] += 1
                    created_count['orders'] += 1

        return JsonResponse({
            'success': True,
            'message': 'Sample data created successfully',
            'created': created_count,
            'note': 'Dashboard should now display vehicle tracking data'
        })

    except Exception as e:
        logger.error(f"Error seeding vehicle tracking data: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


def _extract_plate(reference: str) -> str:
    """Extract vehicle plate from reference string"""
    if not reference:
        return None
    import re
    s = str(reference).strip().upper()
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
