#!/bin/bash

# GTFS Real-time Transit Data Platform - Bundle Destruction Script
# This script destroys all deployed Databricks asset bundles in reverse order

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Bundle configurations (in reverse deployment order)
APP_BUNDLE="bundles/app"
DATABASE_BUNDLE="bundles/database"
ETL_BUNDLE="bundles/etl"
INGESTION_JOB_BUNDLE="bundles/ingestion-job"
INFRASTRUCTURE_BUNDLE="bundles/infrastructure"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Databricks CLI is available
check_databricks_cli() {
    if ! command -v databricks &> /dev/null; then
        print_error "Databricks CLI is not installed or not in PATH"
        print_status "Please install it from: https://docs.databricks.com/dev-tools/cli/install.html"
        exit 1
    fi
}

# Function to destroy a specific bundle
destroy_bundle() {
    local bundle_file=$1
    local bundle_name=$2
    
    if [[ ! -d "$bundle_file" ]]; then
        print_warning "$bundle_name bundle directory not found - skipping"
        return 0
    fi
    
    print_status "Destroying $bundle_name bundle..."
    
    local destroy_output
    destroy_output=$(cd "$bundle_file" && databricks bundle destroy --target dev --auto-approve 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        print_success "$bundle_name bundle destroyed successfully"
        return 0
    elif echo "$destroy_output" | grep -q -i "not found\|does not exist\|no resources"; then
        print_success "$bundle_name bundle was not deployed - skipping"
        return 0
    else
        print_error "$bundle_name bundle destruction failed"
        echo "$destroy_output" | tail -5
        return 1
    fi
}

# Function to stop running jobs and pipelines
stop_running_services() {
    print_status "Stopping running services..."
    
    # Stop ingestion job if running
    print_status "Checking for running ingestion jobs..."
    local job_list
    job_list=$(databricks jobs list --output json 2>/dev/null | grep -i "gtfs_rt_ingestion_job" || true)
    if [[ -n "$job_list" ]]; then
        print_status "Found running ingestion job, stopping it..."
        # Note: Jobs can't be stopped via CLI, they need to be cancelled
        print_warning "Ingestion job is running - you may need to cancel it manually in the Databricks UI"
    fi
    
    # Stop ETL pipeline if running
    print_status "Checking for running ETL pipelines..."
    local pipeline_list
    pipeline_list=$(databricks pipelines list --output json 2>/dev/null | grep -i "gtfs_rt_etl" || true)
    if [[ -n "$pipeline_list" ]]; then
        print_status "Found running ETL pipeline, stopping it..."
        local pipeline_id
        pipeline_id=$(echo "$pipeline_list" | jq -r '.pipeline_id' 2>/dev/null || echo "")
        if [[ -n "$pipeline_id" ]]; then
            databricks pipelines stop --pipeline-id "$pipeline_id" 2>/dev/null || true
            print_success "ETL pipeline stopped"
        fi
    fi
}

# Function to show help
show_help() {
    echo "GTFS Real-time Transit Data Platform - Bundle Destruction Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "This script destroys all deployed Databricks asset bundles in reverse order:"
    echo "  1. App (Streamlit application)"
    echo "  2. Synced Tables"
    echo "  3. ETL Pipeline"
    echo "  4. Ingestion Job"
    echo "  5. Infrastructure (schema, volumes, database instance, database catalog)"
    echo ""
    echo "Options:"
    echo "  --help, -h            Show this help message"
    echo "  --force               Force destruction without confirmation"
    echo "  --stop-services       Stop running jobs and pipelines before destruction"
    echo "  --app-only            Destroy only the app bundle"
    echo "  --database-only       Destroy only the synced tables bundle"
    echo "  --etl-only            Destroy only the ETL pipeline bundle"
    echo "  --ingestion-only      Destroy only the ingestion job bundle"
    echo "  --infrastructure-only Destroy only the infrastructure bundle"
    echo ""
    echo "Examples:"
    echo "  $0                      # Full destruction with confirmation"
    echo "  $0 --force              # Full destruction without confirmation"
    echo "  $0 --stop-services      # Stop services first, then destroy all"
    echo "  $0 --app-only           # Destroy only the app bundle"
    echo "  $0 --infrastructure-only # Destroy only the infrastructure bundle"
    exit 0
}

# Main destruction function
main() {
    echo "=============================================================="
    print_status "GTFS Real-time Transit Data Platform - Bundle Destruction"
    echo "=============================================================="
    
    # Check prerequisites
    check_databricks_cli
    
    # Confirm destruction unless --force is used
    if [[ "${FORCE_DESTROY:-false}" != "true" ]]; then
        echo ""
        print_warning "This will destroy ALL deployed bundles and their resources!"
        print_warning "This action cannot be undone."
        echo ""
        read -p "Are you sure you want to continue? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            print_status "Destruction cancelled"
            exit 0
        fi
    fi
    
    # Stop running services if requested
    if [[ "${STOP_SERVICES:-false}" == "true" ]]; then
        stop_running_services
    fi
    
    # Destroy bundles in reverse order
    print_status "Starting bundle destruction in reverse deployment order..."
    echo ""
    
    # Step 1/5: Destroy app bundle
    print_status "Step 1/5: Destroying app bundle..."
    if ! destroy_bundle "$APP_BUNDLE" "App"; then
        print_warning "App bundle destruction failed - continuing with other bundles"
    fi
    echo ""
    
    # Step 2/5: Destroy synced tables bundle
    print_status "Step 2/5: Destroying synced tables bundle..."
    if ! destroy_bundle "$DATABASE_BUNDLE" "Synced Tables"; then
        print_warning "Synced tables bundle destruction failed - continuing with other bundles"
    fi
    echo ""
    
    # Step 3/5: Destroy ETL pipeline bundle
    print_status "Step 3/5: Destroying ETL pipeline bundle..."
    if ! destroy_bundle "$ETL_BUNDLE" "ETL Pipeline"; then
        print_warning "ETL pipeline bundle destruction failed - continuing with other bundles"
    fi
    echo ""
    
    # Step 4/5: Destroy ingestion job bundle
    print_status "Step 4/5: Destroying ingestion job bundle..."
    if ! destroy_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job"; then
        print_warning "Ingestion job bundle destruction failed - continuing with other bundles"
    fi
    echo ""
    
    # Step 5/5: Destroy infrastructure bundle
    print_status "Step 5/5: Destroying infrastructure bundle..."
    if ! destroy_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure"; then
        print_warning "Infrastructure bundle destruction failed"
    fi
    echo ""
    
    # Final status
    echo "=============================================================="
    print_success "Bundle destruction completed!"
    print_status "All deployed bundles have been destroyed."
    print_status "You can now redeploy using: ./deploy.sh"
    echo "=============================================================="
}

# Individual bundle destruction functions
destroy_app_only() {
    print_status "Destroying app bundle only..."
    check_databricks_cli
    destroy_bundle "$APP_BUNDLE" "App"
}

destroy_database_only() {
    print_status "Destroying synced tables bundle only..."
    check_databricks_cli
    destroy_bundle "$DATABASE_BUNDLE" "Synced Tables"
}

destroy_etl_only() {
    print_status "Destroying ETL pipeline bundle only..."
    check_databricks_cli
    destroy_bundle "$ETL_BUNDLE" "ETL Pipeline"
}

destroy_ingestion_only() {
    print_status "Destroying ingestion job bundle only..."
    check_databricks_cli
    destroy_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job"
}

destroy_infrastructure_only() {
    print_status "Destroying infrastructure bundle only..."
    check_databricks_cli
    destroy_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure"
}

# Parse command line arguments
case "${1:-}" in
    --help|-h)
        show_help
        ;;
    --force)
        print_status "Running destruction with force flag..."
        export FORCE_DESTROY=true
        main
        exit $?
        ;;
    --stop-services)
        print_status "Stopping services before destruction..."
        export STOP_SERVICES=true
        main
        exit $?
        ;;
    --app-only)
        destroy_app_only
        exit $?
        ;;
    --database-only)
        destroy_database_only
        exit $?
        ;;
    --etl-only)
        destroy_etl_only
        exit $?
        ;;
    --ingestion-only)
        destroy_ingestion_only
        exit $?
        ;;
    --infrastructure-only)
        destroy_infrastructure_only
        exit $?
        ;;
    "")
        # No arguments, run full destruction
        main
        ;;
    *)
        print_error "Unknown option: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
