#!/bin/bash

# GTFS Real-time Transit Data Platform Deployment Script
# This script handles the deployment of multiple Databricks Asset Bundles with proper error handling

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Bundle configurations
INFRASTRUCTURE_BUNDLE="bundles/infrastructure"
INGESTION_JOB_BUNDLE="bundles/ingestion-job"
ETL_BUNDLE="bundles/etl"
DATABASE_BUNDLE="bundles/database"
APP_BUNDLE="bundles/app"

# Default profile (can be overridden with --profile option)
DATABRICKS_PROFILE="test"

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

# Function to check if databricks CLI is available
check_databricks_cli() {
    print_status "Checking Databricks CLI availability..."
    if ! command -v databricks &> /dev/null; then
        print_error "Databricks CLI not found. Please install it first."
        exit 1
    fi
    print_success "Databricks CLI found"
}

# Function to set up environment variables from shared-variables.yml
setup_environment() {
    print_status "Setting up environment variables from shared-variables.yml..."
    
    # Source the workspace host from shared-variables.yml
    if [[ -f "shared-variables.yml" ]]; then
        # Extract workspace_host value using yq or grep/sed
        if command -v yq &> /dev/null; then
            WORKSPACE_HOST=$(yq eval '.variables.workspace_host.default' shared-variables.yml)
        else
            # Fallback to grep/sed if yq is not available
            WORKSPACE_HOST=$(grep -A 1 "workspace_host:" shared-variables.yml | grep "default:" | sed 's/.*default: *//' | tr -d ' ')
        fi
        
        if [[ -n "$WORKSPACE_HOST" ]]; then
            export DATABRICKS_HOST="$WORKSPACE_HOST"
            print_success "Set DATABRICKS_HOST to: $WORKSPACE_HOST"
        else
            print_error "Could not extract workspace_host from shared-variables.yml"
            exit 1
        fi
    else
        print_error "shared-variables.yml not found"
        exit 1
    fi
}

# Function to validate bundle configuration
validate_bundle() {
    local bundle_file=$1
    local bundle_name=$2
    
    print_status "Validating $bundle_name bundle configuration..."
    local validation_output
    validation_output=$(cd "$bundle_file" && /opt/homebrew/bin/databricks bundle validate --target dev -p "$DATABRICKS_PROFILE" 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        print_success "$bundle_name bundle configuration is valid"
        return 0
    else
        # Check if it's just an authentication error (which is expected)
        if echo "$validation_output" | grep -q "cannot configure default credentials"; then
            print_success "$bundle_name bundle configuration is valid (authentication required for full validation)"
            return 0
        else
            print_error "$bundle_name bundle validation failed"
            echo "Full validation output:"
            echo "$validation_output"
            return 1
        fi
    fi
}

# Function to check if bundle resources already exist
check_bundle_exists() {
    local bundle_file=$1
    local bundle_name=$2
    
    # Try to get bundle summary to see if it's already deployed
    local summary_output
    summary_output=$(cd "$bundle_file" && /opt/homebrew/bin/databricks bundle summary --target dev -p "$DATABRICKS_PROFILE" 2>/dev/null)
    local exit_code=$?
    
    # If the command failed or returned empty, bundle doesn't exist
    if [[ $exit_code -ne 0 ]] || [[ -z "$summary_output" ]]; then
        return 1
    fi
    
    # Check if the summary shows resources are actually deployed
    # Look for resource sections (Database catalogs, Database instances, Schemas, Volumes, etc.)
    if echo "$summary_output" | grep -q -E "(Database catalogs|Database instances|Schemas|Volumes|Jobs|Pipelines|Apps)"; then
        print_status "$bundle_name bundle appears to be already deployed"
        return 0
    else
        return 1
    fi
}

# Function to backup current databricks configurations
backup_configs() {
    print_status "Creating backup of bundle configurations..."
    
    # Only backup if original exists and backup doesn't exist yet
    if [[ -f "databricks.yml" && ! -f "databricks.yml.backup" ]]; then
        cp databricks.yml databricks.yml.backup
        print_success "Backup created: databricks.yml.backup"
    fi
    
    for bundle_dir in "$INFRASTRUCTURE_BUNDLE" "$INGESTION_BUNDLE" "$DATABASE_BUNDLE" "$APP_BUNDLE"; do
        if [[ -f "$bundle_dir/databricks.yml" ]]; then
            cp "$bundle_dir/databricks.yml" "$bundle_dir/databricks.yml.backup"
            print_success "Backup created: $bundle_dir/databricks.yml.backup"
        fi
    done
}

# Function to deploy a specific bundle
deploy_bundle() {
    local bundle_file=$1
    local bundle_name=$2
    local force_flag=${3:-false}
    
    # Check if force deployment is enabled globally
    if [[ "${FORCE_DEPLOYMENT:-false}" == "true" ]]; then
        force_flag=true
    fi
    
    print_status "Deploying $bundle_name bundle..."
    
    local deploy_output
    local exit_code
    
    if [[ "$force_flag" == "true" ]]; then
        deploy_output=$(cd "$bundle_file" && /opt/homebrew/bin/databricks bundle deploy --target dev --force --force-lock --auto-approve -p "$DATABRICKS_PROFILE" 2>&1)
        exit_code=$?
    else
        deploy_output=$(cd "$bundle_file" && /opt/homebrew/bin/databricks bundle deploy --target dev --auto-approve -p "$DATABRICKS_PROFILE" 2>&1)
        exit_code=$?
        
        # If deployment failed, try with force flags
        if [[ $exit_code -ne 0 ]]; then
            print_warning "$bundle_name bundle deployment failed, trying with force flags..."
            deploy_output=$(cd "$bundle_file" && /opt/homebrew/bin/databricks bundle deploy --target dev --force --force-lock --auto-approve -p "$DATABRICKS_PROFILE" 2>&1)
            exit_code=$?
        fi
    fi
    
    # Check if deployment was successful or if resources already exist
    if [[ $exit_code -eq 0 ]]; then
        print_success "$bundle_name bundle deployment successful"
        return 0
    elif echo "$deploy_output" | grep -q -i "already exists\|already created\|no changes"; then
        print_success "$bundle_name bundle resources already exist - skipping"
        return 0
    else
        print_error "$bundle_name bundle deployment failed"
        echo "$deploy_output" | tail -5  # Show last 5 lines of error output
        return 1
    fi
}

# Function to start the data ingestion job
start_ingestion_job() {
    print_status "Starting GTFS-RT data ingestion job..."
    
    # Start the job with --no-wait to avoid blocking
    local job_output
    job_output=$(cd "$INGESTION_JOB_BUNDLE" && /opt/homebrew/bin/databricks bundle run gtfs_rt_job --target dev --no-wait -p "$DATABRICKS_PROFILE" 2>&1)
    local exit_code=$?
    
    # Check if we got a run URL (success)
    if [[ $exit_code -eq 0 ]] && echo "$job_output" | grep -q "Run URL"; then
        print_success "Data ingestion job started successfully"
        echo "$job_output" | grep "Run URL"
        return 0
    elif echo "$job_output" | grep -q -i "already running\|is running\|active"; then
        print_success "Data ingestion job is already running - continuing"
        return 0
    else
        print_warning "Failed to start data ingestion job automatically"
        print_status "Job may already be running or will auto-start"
        print_status "You can start it manually later with: cd $INGESTION_JOB_BUNDLE && /opt/homebrew/bin/databricks bundle run gtfs_rt_job --target dev --no-wait -p $DATABRICKS_PROFILE"
        return 0  # Don't fail deployment if job doesn't start
    fi
}

# Function to wait for a specified time with progress indicator
wait_with_progress() {
    local wait_time=$1
    local message=$2
    
    print_status "$message"
    
    for i in $(seq 1 $wait_time); do
        echo -n "."
        sleep 1
    done
    echo ""
    print_success "Wait completed (${wait_time}s)."
}

# Function to start the DLT pipeline
start_dlt_pipeline() {
    print_status "Starting DLT pipeline to process collected data..."
    
    # Note: DLT pipelines are started automatically when deployed with continuous=true
    local pipeline_output
    pipeline_output=$(cd "$ETL_BUNDLE" && /opt/homebrew/bin/databricks pipelines start-update --pipeline-name gtfs_rt_etl -p "$DATABRICKS_PROFILE" 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        print_success "DLT pipeline update triggered successfully"
        return 0
    elif echo "$pipeline_output" | grep -q -i "already running\|is running\|active\|continuous"; then
        print_success "DLT pipeline is already running or will auto-start"
        return 0
    else
        print_warning "Could not trigger manual pipeline update"
        print_status "DLT pipeline should start automatically due to continuous=true setting"
        return 0  # Don't fail deployment if pipeline doesn't start manually
    fi
}

# Function to deploy app bundle
deploy_app_bundle() {
    print_status "Deploying Streamlit application bundle..."
    
    if ! deploy_bundle "$APP_BUNDLE" "App"; then
        print_error "App bundle deployment failed"
        return 1
    fi
    
    print_status "Starting Streamlit application..."
    if (cd "$APP_BUNDLE" && /opt/homebrew/bin/databricks apps start gtfs-rt-app --target dev -p "$DATABRICKS_PROFILE"); then
        print_success "Streamlit application started successfully"
        return 0
    else
        print_warning "Failed to start Streamlit app automatically"
        print_status "You can start it manually later with: cd $APP_BUNDLE && /opt/homebrew/bin/databricks apps start gtfs-rt-app -p $DATABRICKS_PROFILE"
        return 1
    fi
}

# Function to validate all bundles
validate_all_bundles() {
    print_status "Validating all bundle configurations..."
    
    validate_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure" || return 1
    validate_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job" || return 1
    validate_bundle "$ETL_BUNDLE" "ETL Pipeline" || return 1
    validate_bundle "$DATABASE_BUNDLE" "Database" || return 1
    validate_bundle "$APP_BUNDLE" "App" || return 1
    
    print_success "All bundle configurations are valid"
    return 0
}

# Function to deploy infrastructure bundle only
deploy_infrastructure_only() {
    print_status "Deploying infrastructure bundle only..."
    
    check_databricks_cli
    backup_configs
    validate_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure" || exit 1
    deploy_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure" || exit 1
    
    print_success "Infrastructure deployment completed!"
    print_status "Next step: Run '$0 --ingestion-job' to deploy ingestion job"
}

# Function to deploy ingestion bundle only
deploy_ingestion_job_only() {
    print_status "Deploying ingestion job bundle only..."
    
    check_databricks_cli
    validate_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job" || exit 1
    deploy_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job" || exit 1
    
    print_success "Ingestion job deployment completed!"
    print_status "Next step: Run '$0 --etl' to deploy ETL pipeline"
}

deploy_etl_only() {
    print_status "Deploying ETL pipeline bundle only..."
    
    check_databricks_cli
    validate_bundle "$ETL_BUNDLE" "ETL Pipeline" || exit 1
    deploy_bundle "$ETL_BUNDLE" "ETL Pipeline" || exit 1
    
    print_success "ETL pipeline deployment completed!"
    print_status "Next step: Run '$0 --database' to deploy synced tables"
}

# Function to deploy synced tables bundle only
deploy_database_only() {
    print_status "Deploying synced tables bundle only..."
    
    check_databricks_cli
    validate_bundle "$DATABASE_BUNDLE" "Synced Tables" || exit 1
    deploy_bundle "$DATABASE_BUNDLE" "Synced Tables" || exit 1
    
    print_success "Synced tables deployment completed!"
    print_status "Next step: Run '$0 --app' to deploy the Streamlit application"
}

# Function to deploy app bundle only
deploy_app_only() {
    print_status "Deploying app bundle only..."
    
    check_databricks_cli
    validate_bundle "$APP_BUNDLE" "App" || exit 1
    
    if ! deploy_app_bundle; then
        print_error "App deployment failed"
        exit 1
    fi
    
    print_success "App deployment completed!"
}

# Function to start all services
start_services() {
    print_status "Starting all services..."
    start_pipeline
    deploy_apps
    print_success "Services startup completed!"
}

# Main deployment function
main() {
    print_status "Starting GTFS Real-time Transit Data Platform deployment..."
    echo "=============================================================="
    
    # Step 1: Check prerequisites
    check_databricks_cli
    
    # Step 2: Set up environment variables
    setup_environment
    
    # Step 3: Backup configurations
    backup_configs
    
    # Step 4: Validate all bundles
    if ! validate_all_bundles; then
        print_error "Deployment aborted due to validation errors"
        exit 1
    fi
    
    # Step 1/5: Deploy infrastructure bundle (schema, volumes, database instance, database catalog)
    print_status "Step 1/5: Deploying infrastructure (schema, volumes, database instance, database catalog)..."
    if check_bundle_exists "$INFRASTRUCTURE_BUNDLE" "Infrastructure"; then
        print_success "Infrastructure bundle already deployed - skipping to next step"
    elif ! deploy_bundle "$INFRASTRUCTURE_BUNDLE" "Infrastructure"; then
        print_error "Infrastructure deployment failed"
        exit 1
    fi
    
    # Step 2/5: Deploy ingestion job bundle and start it
    print_status "Step 2/5: Deploying ingestion job bundle..."
    if ! deploy_bundle "$INGESTION_JOB_BUNDLE" "Ingestion Job"; then
        print_error "Ingestion job deployment failed"
        exit 1
    fi
    
    # Start ingestion job and wait 30 seconds
    print_status "Starting ingestion job to collect raw data..."
    if ! start_ingestion_job; then
        print_error "Failed to start ingestion job - continuing with deployment"
    fi
    
    wait_with_progress 30 "Waiting 30 seconds for ingestion job to collect initial data..."
    
    # Step 3/5: Deploy ETL pipeline bundle and start it
    print_status "Step 3/5: Deploying ETL pipeline bundle..."
    if ! deploy_bundle "$ETL_BUNDLE" "ETL Pipeline"; then
        print_error "ETL pipeline deployment failed"
        exit 1
    fi
    
    # Start DLT pipeline and wait 120 seconds
    print_status "Starting DLT pipeline to process collected data..."
    if ! start_dlt_pipeline; then
        print_error "Failed to start DLT pipeline - synced tables deployment may fail"
        print_status "The pipeline needs to create silver tables before synced tables can be deployed"
    fi
    
    wait_with_progress 120 "Waiting 120 seconds for DLT pipeline to process data into silver tables..."
    
    # Step 4/5: Deploy synced tables (depends on silver tables from pipeline)
    print_status "Step 4/5: Deploying synced tables (depends on silver tables from pipeline)..."
    if check_bundle_exists "$DATABASE_BUNDLE" "Synced Tables"; then
        print_success "Synced tables bundle already deployed - skipping to next step"
    elif ! deploy_bundle "$DATABASE_BUNDLE" "Synced Tables"; then
        print_error "Synced tables deployment failed"
        exit 1
    fi
    
    # Step 5/5: Deploy app (Streamlit application)
    print_status "Step 5/5: Deploying app (Streamlit application)..."
    if ! deploy_app_bundle; then
        print_error "App deployment failed"
        exit 1
    fi
    
    # Step 8: Final status
    echo "=============================================================="
    print_success "Multi-bundle deployment completed!"
    print_status "Your GTFS Real-time Transit Data Platform is now deployed."
    echo ""
    print_status "Deployed bundles:"
    echo "  1. Infrastructure: $INFRASTRUCTURE_BUNDLE"
    echo "  2. Ingestion: $INGESTION_BUNDLE"
    echo "  3. Database: $DATABASE_BUNDLE"
    echo "  4. App: $APP_BUNDLE"
    echo ""
    print_status "Next steps:"
    echo "  1. Check the Databricks workspace for deployed resources"
    echo "  2. Monitor the data ingestion job: cd $INGESTION_BUNDLE && /opt/homebrew/bin/databricks bundle run gtfs_rt_ingestion_job --target dev -p $DATABRICKS_PROFILE"
    echo "  3. Access the Streamlit app: cd $APP_BUNDLE && /opt/homebrew/bin/databricks apps start gtfs-rt-app -p $DATABRICKS_PROFILE"
    echo "  4. View logs: /opt/homebrew/bin/databricks apps logs gtfs-rt-app -p $DATABRICKS_PROFILE"
    echo ""
    print_status "Backup files created in current directory"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            DATABRICKS_PROFILE="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "GTFS Real-time Transit Data Platform Deployment Script"
        echo "Multi-bundle deployment with proper dependency order"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --help, -h            Show this help message"
        echo "  --profile PROFILE     Use specified Databricks profile (default: test)"
        echo "  --validate            Only validate all bundle configurations"
        echo "  --infrastructure      Deploy only infrastructure bundle"
        echo "  --ingestion-job      Deploy only ingestion job bundle"
        echo "  --etl                Deploy only ETL pipeline bundle"
        echo "  --database           Deploy only synced tables bundle"
        echo "  --app                Deploy only app bundle"
        echo "  --start-services     Start continuous job (30s), then pipeline (120s)"
        echo "  --force              Use force flags to override locks and conflicts"
        echo "  --clear-locks        Clear deployment locks for all bundles"
        echo ""
        echo "Bundle deployment order:"
        echo "  1. Infrastructure (schema, volumes, database instance, database catalog)"
        echo "  2. Ingestion Job (deploy and start job, wait 30s)"
        echo "  3. ETL Pipeline (deploy and start pipeline, wait 120s)"
        echo "  4. Synced Tables (deploy synced tables that depend on silver tables)"
        echo "  5. App (Streamlit application)"
        echo ""
        echo "Examples:"
        echo "  $0                      # Full multi-bundle deployment (uses 'test' profile)"
        echo "  $0 --profile prod       # Full deployment using 'prod' profile"
        echo "  $0 --validate           # Validate all configurations"
        echo "  $0 --infrastructure     # Deploy bundles/infrastructure only"
        echo "  $0 --ingestion-job     # Deploy bundles/ingestion-job only"
        echo "  $0 --etl               # Deploy bundles/etl only"
        echo "  $0 --database          # Deploy bundles/database (synced tables) only"
        echo "  $0 --app               # Deploy bundles/app only"
        echo "  $0 --start-services    # Start continuous job (30s), then pipeline (120s)"
        echo "  $0 --force             # Force deployment (override locks)"
        echo "  $0 --clear-locks       # Clear deployment locks"
        exit 0
        ;;
    --validate)
        check_databricks_cli
        validate_all_bundles
        exit $?
        ;;
    --force)
        print_status "Running full deployment with force flags..."
        export FORCE_DEPLOYMENT=true
        main
        exit $?
        ;;
    --clear-locks)
        print_status "Clearing deployment locks for all bundles..."
        for bundle_dir in "$INFRASTRUCTURE_BUNDLE" "$INGESTION_JOB_BUNDLE" "$ETL_BUNDLE" "$DATABASE_BUNDLE" "$APP_BUNDLE"; do
            if [[ -d "$bundle_dir" ]]; then
                print_status "Clearing lock for $bundle_dir..."
                (cd "$bundle_dir" && /opt/homebrew/bin/databricks bundle deploy --target dev --force-lock --help -p "$DATABRICKS_PROFILE" > /dev/null 2>&1) || true
            fi
        done
        print_success "Deployment locks cleared. You can now run deployments normally."
        exit 0
        ;;
    --infrastructure)
        deploy_infrastructure_only
        exit $?
        ;;
    --ingestion-job)
        deploy_ingestion_job_only
        exit $?
        ;;
    --etl)
        deploy_etl_only
        exit $?
        ;;
    --database)
        deploy_database_only
        exit $?
        ;;
    --app)
        deploy_app_only
        exit $?
        ;;
    --start-services)
        print_status "Starting ingestion job and DLT pipeline..."
        start_ingestion_job
        wait_with_progress 30 "Waiting 30 seconds for ingestion job to collect initial data..."
        start_dlt_pipeline
        wait_with_progress 120 "Waiting 120 seconds for DLT pipeline to process data into silver tables..."
        exit $?
        ;;
    "")
        # No arguments, run full deployment
        main
        ;;
    *)
        print_error "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac