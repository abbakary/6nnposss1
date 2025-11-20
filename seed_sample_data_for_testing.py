#!/usr/bin/env python
"""
Seed script to create sample data with 10 customers and 20 orders.
Demonstrates all order statuses correctly:
- created: Orders just created (0-5 minutes old)
- in_progress: Orders auto-progressed after 10 minutes
- overdue: Orders that have been in progress for 9+ working hours
- completed: Finished orders with completion timestamps
- cancelled: Cancelled orders with cancellation reasons
"""

import os
import sys
import django
from datetime import datetime, timedelta
from decimal import Decimal
import random

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pos_tracker.settings')
django.setup()

from tracker.models import Customer, Vehicle, Order, Branch, InventoryItem, Brand
from django.utils import timezone
from django.contrib.auth.models import User

def get_or_create_branch():
    """Get or create default branch"""
    branch, _ = Branch.objects.get_or_create(
        code='MAIN',
        defaults={'name': 'Main Branch', 'is_active': True}
    )
    return branch

def get_or_create_user():
    """Get or create a staff user for order assignment"""
    user, _ = User.objects.get_or_create(
        username='staff_user',
        defaults={
            'email': 'staff@example.com',
            'first_name': 'Staff',
            'last_name': 'User',
            'is_staff': True
        }
    )
    return user

def create_sample_data():
    print("=" * 70)
    print("SEEDING SAMPLE DATA: 10 Customers and 20 Orders with All Statuses")
    print("=" * 70)
    
    branch = get_or_create_branch()
    staff_user = get_or_create_user()
    now = timezone.now()
    
    # Sample customer data
    customers_data = [
        {'name': 'John Smith', 'phone': '0701234567', 'email': 'john.smith@email.com', 'type': 'personal'},
        {'name': 'Sarah Johnson', 'phone': '0701234568', 'email': 'sarah.j@email.com', 'type': 'personal'},
        {'name': 'Michael Brown', 'phone': '0701234569', 'email': 'mike.brown@email.com', 'type': 'personal'},
        {'name': 'Uganda Revenue Authority', 'phone': '0701234570', 'email': 'fleet@ura.go.ug', 'type': 'government'},
        {'name': 'Red Cross Uganda', 'phone': '0701234571', 'email': 'logistics@redcross.ug', 'type': 'ngo'},
        {'name': 'MTN Uganda Ltd', 'phone': '0701234572', 'email': 'fleet@mtn.co.ug', 'type': 'company'},
        {'name': 'David Wilson', 'phone': '0701234573', 'email': 'david.w@email.com', 'type': 'personal'},
        {'name': 'Ministry of Health', 'phone': '0701234574', 'email': 'transport@health.go.ug', 'type': 'government'},
        {'name': 'UNICEF Uganda', 'phone': '0701234575', 'email': 'ops@unicef.org', 'type': 'ngo'},
        {'name': 'Grace Nakato', 'phone': '0701234576', 'email': 'grace.n@email.com', 'type': 'personal'},
    ]
    
    # Create customers
    print("\n[1] Creating 10 Customers...")
    customers = []
    for i, customer_data in enumerate(customers_data, 1):
        customer, created = Customer.objects.get_or_create(
            phone=customer_data['phone'],
            branch=branch,
            defaults={
                'full_name': customer_data['name'],
                'email': customer_data['email'],
                'customer_type': customer_data['type'],
                'registration_date': now - timedelta(days=random.randint(30, 365)),
                'address': f"Plot {random.randint(100, 999)}, Kampala"
            }
        )
        customers.append(customer)
        status = "✓ Created" if created else "✓ Already exists"
        print(f"  {i:2d}. {customer.full_name:30s} ({customer_data['type']:12s}) {status}")
    
    # Create vehicles for each customer
    print("\n[2] Creating Vehicles for Customers...")
    vehicles = []
    vehicle_makes = ['Toyota', 'Honda', 'Nissan', 'Mercedes', 'Isuzu']
    vehicle_models = ['Camry', 'Hilux', 'Civic', 'X-Trail', 'Actros']
    
    for customer in customers:
        num_vehicles = random.randint(1, 2)
        for _ in range(num_vehicles):
            plate = f"U{random.randint(100, 999)}{random.choice('ABCDE')}"
            vehicle, created = Vehicle.objects.get_or_create(
                plate_number=plate,
                customer=customer,
                defaults={
                    'make': random.choice(vehicle_makes),
                    'model': random.choice(vehicle_models),
                    'vehicle_type': 'sedan'
                }
            )
            vehicles.append(vehicle)
            if created:
                print(f"  ✓ {vehicle.plate_number:10s} for {customer.full_name}")
    
    # Create brands and inventory items for sales orders
    print("\n[3] Creating Brands and Inventory Items...")
    brands_data = [
        {'name': 'Michelin', 'description': 'Premium tires'},
        {'name': 'Bridgestone', 'description': 'Quality tires'},
        {'name': 'Goodyear', 'description': 'Trusted brand'},
    ]
    
    brands = []
    for brand_data in brands_data:
        brand, created = Brand.objects.get_or_create(
            name=brand_data['name'],
            defaults={'description': brand_data['description']}
        )
        brands.append(brand)
        if created:
            print(f"  ✓ Brand: {brand.name}")
    
    inventory_items = []
    tire_types = ['All Season', 'Summer', 'Winter', 'Performance']
    sizes = ['195/65R15', '205/55R16', '225/45R17']
    
    for brand in brands:
        for i, tire_type in enumerate(tire_types[:2]):
            item_name = f"{tire_type} {sizes[i]}"
            item, created = InventoryItem.objects.get_or_create(
                name=item_name,
                brand=brand,
                defaults={
                    'quantity': random.randint(20, 100),
                    'price': Decimal(str(random.randint(150, 300))),
                    'reorder_level': 5
                }
            )
            inventory_items.append(item)
            if created:
                print(f"  ✓ {brand.name} - {item_name}")
    
    # Create 20 orders with different statuses
    print("\n[4] Creating 20 Orders with All Statuses...")
    print("-" * 70)
    
    orders_by_status = {
        'created': [],
        'in_progress': [],
        'overdue': [],
        'completed': [],
        'cancelled': []
    }
    
    # Helper to calculate timestamps for orders
    def create_timestamps_for_status(status, base_days_ago=0):
        """Create appropriate timestamps based on order status"""
        created = now - timedelta(days=base_days_ago, minutes=random.randint(1, 30))
        started = None
        completed = None
        cancelled = None
        
        if status == 'created':
            # Order created 0-5 minutes ago, not yet auto-progressed
            created = now - timedelta(minutes=random.randint(1, 5))
        
        elif status == 'in_progress':
            # Order created 15+ minutes ago, auto-progressed to in_progress
            created = now - timedelta(minutes=random.randint(15, 120))
            started = created + timedelta(minutes=10)  # Will be set by middleware after 10 mins
        
        elif status == 'overdue':
            # Order created 9+ working hours ago
            # Working hours: 8 AM to 5 PM (9 hours/day)
            # Create order at 8 AM yesterday or earlier
            yesterday_8am = (now - timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
            created = yesterday_8am - timedelta(minutes=random.randint(1, 30))
            started = created + timedelta(minutes=10)
            # Status will be 'overdue' due to middleware calculation
        
        elif status == 'completed':
            # Order created 1-30 days ago and completed
            created = now - timedelta(days=random.randint(1, 30))
            started = created + timedelta(minutes=10)
            completed = started + timedelta(hours=random.randint(2, 8))
        
        elif status == 'cancelled':
            # Order created 1-20 days ago and cancelled
            created = now - timedelta(days=random.randint(1, 20))
            started = created + timedelta(minutes=10)
            cancelled = started + timedelta(minutes=random.randint(30, 240))
        
        return created, started, completed, cancelled
    
    # Create orders: 4 of each status + 1 overdue = 20 orders
    order_configs = [
        # created status: 4 orders
        ('created', 0), ('created', 0), ('created', 0), ('created', 0),
        # in_progress status: 4 orders
        ('in_progress', 0), ('in_progress', 0), ('in_progress', 0), ('in_progress', 0),
        # overdue status: 4 orders (special: created 9+ hours ago)
        ('overdue', 0), ('overdue', 0), ('overdue', 0), ('overdue', 0),
        # completed status: 4 orders
        ('completed', 5), ('completed', 10), ('completed', 20), ('completed', 30),
        # cancelled status: 4 orders
        ('cancelled', 3), ('cancelled', 7), ('cancelled', 15), ('cancelled', 25),
    ]
    
    service_descriptions = [
        'Oil change and filter replacement',
        'Brake pad replacement and inspection',
        'Tire rotation and balancing',
        'Engine tune-up and diagnostics',
        'Transmission fluid service'
    ]
    
    order_num = 0
    for status, base_days_ago in order_configs:
        order_num += 1
        
        # Select random customer and vehicle
        customer = random.choice(customers)
        vehicle = random.choice(customer.vehicles.all()) if customer.vehicles.exists() else None
        order_type = random.choice(['service', 'sales', 'service'])
        priority = random.choice(['low', 'medium', 'high', 'urgent'])
        
        # Generate timestamps
        created_at, started_at, completed_at, cancelled_at = create_timestamps_for_status(status, base_days_ago)
        
        order_data = {
            'customer': customer,
            'vehicle': vehicle,
            'branch': branch,
            'type': order_type,
            'status': status,
            'priority': priority,
            'created_at': created_at,
            'estimated_duration': random.randint(60, 480),
            'assigned_to': staff_user if random.choice([True, False]) else None,
        }
        
        # Set timestamps
        if started_at:
            order_data['started_at'] = started_at
        if completed_at:
            order_data['completed_at'] = completed_at
            order_data['completion_date'] = completed_at
        if cancelled_at:
            order_data['cancelled_at'] = cancelled_at
            order_data['cancellation_reason'] = random.choice([
                'Customer request',
                'Parts unavailable',
                'Weather conditions',
                'Customer no-show'
            ])
        
        # Type-specific data
        if order_type == 'service':
            order_data['description'] = random.choice(service_descriptions)
        elif order_type == 'sales':
            item = random.choice(inventory_items)
            order_data['item_name'] = item.name
            order_data['brand'] = item.brand.name
            order_data['quantity'] = random.randint(1, 4)
            order_data['tire_type'] = 'New'
        
        try:
            order = Order.objects.create(**order_data)
            orders_by_status[status].append(order)
            
            # Update customer visit tracking
            customer.total_visits = (customer.total_visits or 0) + 1
            customer.last_visit = created_at
            customer.save(update_fields=['total_visits', 'last_visit'])
            
            status_display = status.replace('_', ' ').upper().ljust(12)
            print(f"{order_num:2d}. {order.order_number:20s} | {customer.full_name:25s} | {status_display} | {order_type.upper()}")
        except Exception as e:
            print(f"  ✗ Error creating order: {e}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nCustomers Created: {len(customers)}")
    print(f"Vehicles Created:  {len(vehicles)}")
    print(f"Total Orders:      {sum(len(orders) for orders in orders_by_status.values())}")
    
    print("\nOrder Distribution by Status:")
    for status, orders in orders_by_status.items():
        print(f"  {status.replace('_', ' ').title():12s} : {len(orders):2d} orders")
    
    print("\nOrder Distribution by Type:")
    service_count = Order.objects.filter(type='service').count()
    sales_count = Order.objects.filter(type='sales').count()
    inquiry_count = Order.objects.filter(type='inquiry').count()
    print(f"  Service    : {service_count:2d} orders")
    print(f"  Sales      : {sales_count:2d} orders")
    print(f"  Inquiry    : {inquiry_count:2d} orders")
    
    print("\nCustomer Information:")
    for customer in customers:
        visit_count = customer.total_visits
        order_count = customer.orders.count()
        print(f"  {customer.full_name:30s} | Visits: {visit_count:2d} | Orders: {order_count:2d}")
    
    print("\n" + "=" * 70)
    print("✓ Sample data seeding completed successfully!")
    print("=" * 70)

if __name__ == '__main__':
    try:
        create_sample_data()
    except Exception as e:
        print(f"\n✗ Error during data seeding: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
