import great_expectations as gx
import pandas as pd
import boto3
import io

# Load the enriched data from S3
s3 = boto3.client('s3', region_name='us-east-1')

obj = s3.get_object(
    Bucket='rohan-data-pipeline-2024',
    Key='processed/orders_enriched.csv'
)
df = pd.read_csv(io.BytesIO(obj['Body'].read()))
print(f'Loaded {len(df)} rows for quality checks')

# Create Great Expectations context
context = gx.get_context()

# Create a data source
data_source = context.data_sources.add_pandas("pandas_source")
data_asset = data_source.add_dataframe_asset(name="orders_enriched")
batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# Create expectation suite
suite = context.suites.add(gx.ExpectationSuite(name="orders_quality_checks"))

# Define our quality rules
expectations = [
    # order_id should never be null
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
    
    # total_amount should always be positive
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="total_amount", min_value=0
    ),
    
    # status should only be valid values
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["completed", "pending", "cancelled", "refunded"]
    ),
    
    # customer_id should never be null
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
    
    # quantity should be positive
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="quantity", min_value=1
    ),
    
    # should have all expected columns
    gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=["order_id", "customer_id", "product_id", "quantity",
                   "status", "order_date", "total_amount", "order_size",
                   "customer_name", "email", "city", "country",
                   "signup_date", "product_name", "category", "price", "stock"]
    ),
]

for exp in expectations:
    suite.add_expectation(exp)

# Run the validation
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="orders_validation",
        data=batch_definition,
        suite=suite
    )
)

results = validation_definition.run(batch_parameters={"dataframe": df})

# Print results
print('\n' + '='*50)
print('DATA QUALITY RESULTS')
print('='*50)

success_count = 0
fail_count = 0

for result in results.results:
    status = "PASS" if result.success else "FAIL"
    expectation = result.expectation_config.type
    if result.success:
        success_count += 1
    else:
        fail_count += 1
    print(f'{status} | {expectation}')

print('='*50)
print(f'Total checks: {success_count + fail_count}')
print(f'Passed: {success_count}')
print(f'Failed: {fail_count}')
print(f'Overall: {"PASSED" if fail_count == 0 else "FAILED"}')