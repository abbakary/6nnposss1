#!/usr/bin/env python
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pos_tracker.settings')
django.setup()

from tracker.models import Vehicle, Invoice, Customer, Order, Branch
from django.utils import timezone
from datetime import timedelta

print("=== DATABASE DATA CHECK ===")
print(f"Total Branches: {Branch.objects.count()}")
print(f"Total Customers: {Customer.objects.count()}")
print(f"Total Vehicles: {Vehicle.objects.count()}")
print(f"Total Invoices: {Invoice.objects.count()}")
print(f"Total Orders: {Order.objects.count()}")

# Check recent invoices
recent_invoices = Invoice.objects.all().order_by('-invoice_date')[:5]
if recent_invoices:
    print(f"\n=== Recent Invoices ===")
    for inv in recent_invoices:
        print(f"  Invoice: {inv.invoice_number}, Date: {inv.invoice_date}, Vehicle: {inv.vehicle}, Branch: {inv.branch}, Total: {inv.total_amount}")
else:
    print("\nNo invoices found in database")

# Check vehicles
vehicles = Vehicle.objects.all()[:5]
if vehicles:
    print(f"\n=== Sample Vehicles ===")
    for v in vehicles:
        inv_count = v.invoices.count()
        print(f"  Plate: {v.plate_number}, Customer: {v.customer.full_name}, Make: {v.make}, Invoices: {inv_count}")
else:
    print("\nNo vehicles found in database")

# Check date range
print(f"\n=== Current Date ===")
today = timezone.now().date()
thirty_days_ago = today - timedelta(days=30)
print(f"Today: {today}")
print(f"30 days ago: {thirty_days_ago}")

# Check invoices in the default range
invoices_in_range = Invoice.objects.filter(invoice_date__range=[thirty_days_ago, today])
print(f"Invoices in default range (30 days): {invoices_in_range.count()}")

# Check vehicles with invoices in range
vehicles_with_invoices = Vehicle.objects.filter(invoices__invoice_date__range=[thirty_days_ago, today]).distinct()
print(f"Vehicles with invoices in default range: {vehicles_with_invoices.count()}")

# Sample branches
branches = Branch.objects.all()
if branches:
    print(f"\n=== Branches ===")
    for b in branches:
        print(f"  {b.name} ({b.code})")
