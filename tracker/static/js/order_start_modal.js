/**
 * Order Start Modal - Multi-step form handler
 * Manages order type selection, customer type selection, and extracted data form
 */

class OrderStartModal {
  constructor() {
    this.modal = null;
    this.currentStep = 0;
    this.totalSteps = 3;
    this.formData = {};
    this.foundCustomer = null;
    this.foundVehicle = null;
    this.init();
  }

  init() {
    this.modal = new bootstrap.Modal(document.getElementById('orderStartModal'), {
      backdrop: 'static',
      keyboard: false
    });

    this.attachEventListeners();
  }

  attachEventListeners() {
    const self = this;

    // Quick lookup functionality
    const quickSearchBtn = document.getElementById('quickSearchBtn');
    const skipQuickStartBtn = document.getElementById('skipQuickStartBtn');
    const quickSearchPlate = document.getElementById('quickSearchPlate');

    if (quickSearchBtn) {
      quickSearchBtn.addEventListener('click', () => self.performQuickLookup());
    }

    if (skipQuickStartBtn) {
      skipQuickStartBtn.addEventListener('click', () => self.skipQuickLookup());
    }

    if (quickSearchPlate) {
      quickSearchPlate.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
          self.performQuickLookup();
        }
      });
    }

    // Continue as new order for existing vehicle
    const continueAsNewBtn = document.getElementById('continueAsNewBtn');
    if (continueAsNewBtn) {
      continueAsNewBtn.addEventListener('click', () => self.continueAsNewOrder());
    }

    // Customer type selection
    document.querySelectorAll('.customer-type-option').forEach(option => {
      option.addEventListener('click', function(e) {
        e.preventDefault();
        const input = this.querySelector('input[type="radio"]');
        input.checked = true;
        self.handleCustomerTypeChange();
      });
    });

    // Form check inputs
    document.querySelectorAll('input[name="customer_type"], input[name="personal_subtype"]').forEach(input => {
      input.addEventListener('change', () => self.handleCustomerTypeChange());
    });

    // Step navigation
    document.getElementById('nextBtn').addEventListener('click', () => self.nextStep());
    document.getElementById('prevBtn').addEventListener('click', () => self.prevStep());
    document.getElementById('submitBtn').addEventListener('click', () => self.submitForm());
    document.getElementById('cancelBtn').addEventListener('click', () => self.resetForm());
  }

  handleCustomerTypeChange() {
    const selectedType = document.querySelector('input[name="customer_type"]:checked')?.value;
    const personalSubtypeSection = document.querySelector('.personal-subtype-section');
    const orgDetailsSection = document.querySelector('.org-details-section');

    if (!selectedType) {
      return;
    }

    // Show/hide personal subtype section
    if (selectedType === 'personal') {
      personalSubtypeSection?.classList.remove('d-none');
      orgDetailsSection?.classList.add('d-none');
      this.clearRequiredFields([
        { name: 'organization_name', container: orgDetailsSection },
        { name: 'tax_number', container: orgDetailsSection }
      ]);
    } else {
      personalSubtypeSection?.classList.add('d-none');
      orgDetailsSection?.classList.remove('d-none');
      document.querySelector('input[name="personal_subtype"]').checked = false;
    }

    this.formData.customer_type = selectedType;
  }

  nextStep() {
    // Validate current step
    if (!this.validateStep(this.currentStep)) {
      return;
    }

    if (this.currentStep < this.totalSteps) {
      this.showStep(this.currentStep + 1);
    }
  }

  prevStep() {
    if (this.currentStep > 0) {
      this.showStep(this.currentStep - 1);
    }
  }

  showStep(stepNumber) {
    // Hide all steps
    document.querySelectorAll('.form-step').forEach(step => {
      step.classList.add('d-none');
    });

    // Remove active class from all step badges
    document.querySelectorAll('.step-badge').forEach(badge => {
      badge.classList.remove('active', 'completed');
    });

    // Show current step and previous completed steps
    for (let i = 0; i <= stepNumber; i++) {
      const step = document.getElementById(`step${i}`);
      const badge = document.querySelector(`.step-badge[data-step="${i}"]`);

      if (i < stepNumber) {
        badge?.classList.add('completed');
      } else if (i === stepNumber) {
        step?.classList.remove('d-none');
        badge?.classList.add('active');
      }
    }

    // Update button visibility
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');
    const submitBtn = document.getElementById('submitBtn');

    prevBtn.style.display = stepNumber === 0 ? 'none' : 'block';
    nextBtn.style.display = stepNumber === this.totalSteps - 1 ? 'none' : 'block';
    submitBtn.style.display = stepNumber === this.totalSteps - 1 ? 'block' : 'none';

    this.currentStep = stepNumber;

    // Auto-fill extracted data if available
    if (stepNumber === 2) {
      this.prepareExtractedDataStep();
    }
  }

  validateStep(stepNumber) {
    this.clearAllErrors();

    switch (stepNumber) {
      case 0:
        // Step 0 (quick lookup) is optional - always return true
        return true;
      case 1:
        return this.validateCustomerType();
      case 2:
        return this.validateExtractedData();
      default:
        return true;
    }
  }

  validateCustomerType() {
    const selected = document.querySelector('input[name="customer_type"]:checked');
    
    if (!selected) {
      this.showError('customerTypeError', 'Please select a customer type');
      return false;
    }

    const type = selected.value;

    // Validate personal subtype if personal customer
    if (type === 'personal') {
      const subtypeSelected = document.querySelector('input[name="personal_subtype"]:checked');
      if (!subtypeSelected) {
        this.showError('customerTypeError', 'Please specify if you are the owner or driver');
        return false;
      }
      this.formData.personal_subtype = subtypeSelected.value;
    }

    // Validate organization details if organizational customer
    if (['company', 'government', 'ngo'].includes(type)) {
      const orgName = document.querySelector('input[name="organization_name"]').value.trim();
      const taxNumber = document.querySelector('input[name="tax_number"]').value.trim();

      if (!orgName) {
        this.showError('customerTypeError', 'Organization name is required');
        return false;
      }

      if (!taxNumber) {
        this.showError('customerTypeError', 'Tax number/TIN is required');
        return false;
      }

      this.formData.organization_name = orgName;
      this.formData.tax_number = taxNumber;
    }

    this.formData.customer_type = type;
    return true;
  }

  validateExtractedData() {
    const errors = [];

    // Validate required fields
    const name = document.querySelector('input[name="extracted_customer_name"]').value.trim();
    const phone = document.querySelector('input[name="extracted_phone"]').value.trim();

    if (!name) {
      errors.push('Customer name is required');
    }

    if (!phone) {
      errors.push('Phone number is required');
    }

    if (errors.length > 0) {
      this.showError('extractedDataError', errors.join('; '));
      return false;
    }

    return true;
  }

  prepareExtractedDataStep() {
    // This is where you would populate extracted data from an upload
    // For now, leave it empty for user input
    // In the future, you could:
    // 1. Accept extracted data via modal parameter
    // 2. Auto-populate from a document upload
    // 3. Load from API response
  }

  submitForm() {
    if (!this.validateExtractedData()) {
      return;
    }

    // Collect all form data
    const formElement = document.getElementById('orderStartForm');
    const formData = new FormData(formElement);

    // Add custom data
    formData.set('customer_name', document.querySelector('input[name="extracted_customer_name"]').value);
    formData.set('phone', document.querySelector('input[name="extracted_phone"]').value);
    formData.set('email', document.querySelector('input[name="extracted_email"]').value || '');
    formData.set('address', document.querySelector('textarea[name="extracted_address"]').value || '');
    formData.set('description', document.querySelector('textarea[name="extracted_description"]').value || '');
    formData.set('estimated_duration', document.querySelector('input[name="extracted_duration"]').value || '');
    formData.set('priority', document.querySelector('select[name="extracted_priority"]').value || 'medium');
    formData.set('plate_number', document.querySelector('input[name="extracted_plate"]').value || '');
    formData.set('vehicle_make', document.querySelector('input[name="extracted_make"]').value || '');
    formData.set('vehicle_model', document.querySelector('input[name="extracted_model"]').value || '');

    // Add force_new_order if it was set
    if (this.formData.force_new_order) {
      formData.set('force_new_order', 'true');
    }

    // Show loading state
    const submitBtn = document.getElementById('submitBtn');
    const originalText = submitBtn.innerHTML;
    submitBtn.disabled = true;
    submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Creating...';

    // Submit to server using CSRF helper
    postWithCSRF('/api/orders/create-from-modal/', formData)
    .then(response => {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      return response.json();
    })
    .then(data => {
      if (data.success) {
        // Show success message
        this.showSuccessMessage('Order created successfully!');

        // Show toast notification
        if (typeof showToast === 'function') {
          showToast(`Order ${data.order_number} created successfully`, 'success');
        }

        // Redirect or close modal
        setTimeout(() => {
          window.location.href = `/tracker/orders/started/${data.order_id}/`;
        }, 1500);
      } else {
        const errorMsg = data.error || 'Failed to create order';
        this.showError('extractedDataError', errorMsg);
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalText;

        // Show toast notification
        if (typeof showToast === 'function') {
          showToast(errorMsg, 'error');
        }
      }
    })
    .catch(error => {
      const errorMsg = 'An error occurred: ' + (error.message || 'Unknown error');
      console.error('Error creating order:', error);
      this.showError('extractedDataError', errorMsg);
      submitBtn.disabled = false;
      submitBtn.innerHTML = originalText;

      // Show toast notification
      if (typeof showToast === 'function') {
        showToast('Failed to create order. Please try again.', 'error');
      }
    });
  }

  resetForm() {
    document.getElementById('orderStartForm').reset();
    this.formData = {};
    this.foundCustomer = null;
    this.foundVehicle = null;
    this.currentStep = 0;
    this.showStep(0);
  }

  showError(elementId, message) {
    const errorDiv = document.getElementById(elementId);
    if (errorDiv) {
      errorDiv.querySelector('span').textContent = message;
      errorDiv.style.display = 'block';
    }
  }

  clearError(elementId) {
    const errorDiv = document.getElementById(elementId);
    if (errorDiv) {
      errorDiv.style.display = 'none';
    }
  }

  clearAllErrors() {
    document.querySelectorAll('[id$="Error"]').forEach(error => {
      error.style.display = 'none';
    });
  }

  clearRequiredFields(fields) {
    fields.forEach(field => {
      if (field.container) {
        const input = field.container.querySelector(`[name="${field.name}"]`);
        if (input) {
          input.value = '';
          input.removeAttribute('required');
        }
      }
    });
  }

  showSuccessMessage(message) {
    const alert = document.createElement('div');
    alert.className = 'alert alert-success alert-dismissible fade show';
    alert.role = 'alert';
    alert.innerHTML = `
      <i class="fa fa-check-circle me-2"></i>${message}
      <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.body.insertBefore(alert, document.body.firstChild);
  }

  performQuickLookup() {
    const plate = document.getElementById('quickSearchPlate').value.trim().toUpperCase();

    // Hide previous results
    document.getElementById('quickLookupResult').style.display = 'none';
    document.getElementById('plateNotFoundAlert').style.display = 'none';
    document.getElementById('quickLookupError').style.display = 'none';

    if (!plate) {
      this.showQuickLookupError('Please enter a vehicle plate number');
      return;
    }

    // Show loading state
    const btn = document.getElementById('quickSearchBtn');
    const originalText = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<i class="fa fa-spinner fa-spin me-2"></i>Searching...';

    // Call API to check plate
    fetch('/api/orders/check-plate/', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': getCSRFToken() || '',
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: JSON.stringify({ plate_number: plate })
    })
    .then(r => r.json())
    .then(data => {
      btn.disabled = false;
      btn.innerHTML = originalText;

      if (data.found && data.customer && data.vehicle) {
        // Show existing customer info
        document.getElementById('existingCustomerName').textContent = data.customer.full_name;
        document.getElementById('existingVehicleInfo').textContent = `${data.vehicle.make} ${data.vehicle.model} (${data.vehicle.plate})`;
        document.getElementById('existingCustomerPhone').textContent = data.customer.phone;
        document.getElementById('existingOrderStatus').textContent = 'Existing Customer';
        document.getElementById('quickLookupResult').style.display = 'block';

        // Store customer info for later use
        this.foundCustomer = data.customer;
        this.foundVehicle = data.vehicle;
      } else {
        // No existing record found
        document.getElementById('plateNotFoundAlert').style.display = 'block';
        this.foundCustomer = null;
        this.foundVehicle = null;
      }
    })
    .catch(error => {
      btn.disabled = false;
      btn.innerHTML = originalText;
      console.error('Lookup error:', error);
      this.showQuickLookupError('Error checking plate number. Please try again.');
    });
  }

  skipQuickLookup() {
    // Clear plate info and move to next step
    this.foundCustomer = null;
    this.foundVehicle = null;
    this.showStep(1);
  }

  continueAsNewOrder() {
    // Customer and vehicle are found - check if existing order exists before proceeding
    if (this.foundCustomer && this.foundVehicle) {
      const plate = this.foundVehicle.plate || '';

      // Check if order exists for this plate
      const checkPayload = {
        plate_number: plate,
        order_type: 'service',
        use_existing_customer: true,
        existing_customer_id: this.foundCustomer.id,
        force_new_order: false
      };

      fetch('/tracker/api/orders/start/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': getCSRFToken() || '',
          'X-Requested-With': 'XMLHttpRequest'
        },
        body: JSON.stringify(checkPayload)
      })
      .then(r => r.json())
      .then(data => {
        if (data && data.success && data.existing_order) {
          // Existing order found - ask user what they want to do
          const choice = confirm(
            'An order for plate ' + plate + ' (Order #' + data.order_number + ') is already started.\n\n' +
            'Click OK to continue with the existing order, or Cancel to start a new order with this plate.'
          );

          if (choice) {
            // Continue with existing order - redirect to it
            window.location.href = '/tracker/orders/started/' + data.order_id + '/';
          } else {
            // Start new order - set force_new_order and pre-populate fields
            this.formData.customer_id = this.foundCustomer.id;
            this.formData.use_existing_customer = true;
            this.formData.force_new_order = true;

            // Pre-populate vehicle info
            document.querySelector('input[name="extracted_plate"]').value = this.foundVehicle.plate || '';
            document.querySelector('input[name="extracted_make"]').value = this.foundVehicle.make || '';
            document.querySelector('input[name="extracted_model"]').value = this.foundVehicle.model || '';

            // Pre-select the customer type
            const customerType = this.foundCustomer.customer_type || 'personal';
            const customerTypeInput = document.querySelector(`input[name="customer_type"][value="${customerType}"]`);
            if (customerTypeInput) {
              customerTypeInput.checked = true;
              this.handleCustomerTypeChange();
            }

            // Move to customer type selection
            this.showStep(1);
          }
        } else {
          // No existing order - proceed to create new order
          this.formData.customer_id = this.foundCustomer.id;
          this.formData.use_existing_customer = true;
          this.formData.force_new_order = false;

          // Pre-populate vehicle info
          document.querySelector('input[name="extracted_plate"]').value = this.foundVehicle.plate || '';
          document.querySelector('input[name="extracted_make"]').value = this.foundVehicle.make || '';
          document.querySelector('input[name="extracted_model"]').value = this.foundVehicle.model || '';

          // Pre-select the customer type
          const customerType = this.foundCustomer.customer_type || 'personal';
          const customerTypeInput = document.querySelector(`input[name="customer_type"][value="${customerType}"]`);
          if (customerTypeInput) {
            customerTypeInput.checked = true;
            this.handleCustomerTypeChange();
          }

          // Move to customer type selection
          this.showStep(1);
        }
      })
      .catch(error => {
        console.error('Error checking for existing order:', error);
        this.showQuickLookupError('Error checking for existing order. Please try again.');
      });
    }
  }

  showQuickLookupError(message) {
    const errorDiv = document.getElementById('quickLookupError');
    if (errorDiv) {
      document.getElementById('quickLookupErrorText').textContent = message;
      errorDiv.style.display = 'block';
    }
  }

  open() {
    this.resetForm();
    this.modal.show();
  }

  close() {
    this.modal.hide();
  }
}

// Initialize on document ready
document.addEventListener('DOMContentLoaded', function() {
  window.orderStartModal = new OrderStartModal();

  // Open modal on button click
  const openModalBtn = document.getElementById('openOrderStartModal');
  if (openModalBtn) {
    openModalBtn.addEventListener('click', () => window.orderStartModal.open());
  }
});
