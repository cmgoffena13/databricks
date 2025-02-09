from pyspark.sql.functions import col, countDistinct

def analyze_rle_compression(df, filter_condition, verbose=False):
    """
    Analyzes the RLE (Run-Length Encoding) compression potential of a DataFrame by examining column cardinalities 
    and the distribution of top values in each column. Optionally filters data based on the provided filter condition.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to analyze.
        filter_condition (str or None): A SQL-style filter condition to apply to the DataFrame. If None, the entire DataFrame is analyzed. Ex. "event_date = '2024-01-01'"
        verbose (bool, optional): Whether to display verbose output, showing the top frequent values per column. Defaults to False.

    Returns:
        None: Prints the analysis results directly.

    Steps:
        1. Cardinality analysis: Counts the distinct values in each column and sorts them by ascending cardinality.
        2. Frequency distribution: Optionally shows the top 5 most frequent values for each column if verbose is True.
        3. Recommended sort order: Prints the recommended column order for Run-Length Encoding (RLE) compression based on cardinality.
    """
    if filter_condition:
        partition_data = df.filter(filter_condition)
    else:
        print("No filter condition provided. Analyzing entire table!!!")
        partition_data = df

    columns = df.columns
    print(f"Analyzing table with filter: {filter_condition}\n")

    # Step 1: Check cardinality for each column
    cardinality_expr = [countDistinct(col(c)).alias(c) for c in columns]
    cardinality_df = partition_data.agg(*cardinality_expr)

    # Collect results and print
    cardinality = cardinality_df.collect()[0].asDict()

    # Reset columns array to have sorted list later
    columns = []

    # Sort the dictionary by cardinality (ascending order)
    sorted_cardinality = sorted(cardinality.items(), key=lambda x: x[1])

    for column, unique_count in sorted_cardinality:
        columns.append(column)
        print(f"{column}: {unique_count} unique values")

    # Step 2: Check frequency distribution for key columns
    if verbose:
        print("\nVerbose Output\n")
        print("Top frequent values per column:")
        for column in columns:
            top_values = (
                partition_data.groupBy(column)
                .count()
                .orderBy("count", ascending=False)
                .limit(5)
            )
            print(f"\nTop values for {column}:")
            top_values.show()

    # Step 3: Identify best columns for sorting (low cardinality first)
    print("\nRecommended sort order for RLE compression:")
    sorted_columns = [column for column, _ in sorted_cardinality]
    print(" -> ".join(sorted_columns))