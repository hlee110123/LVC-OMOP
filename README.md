# Early PVI Cohort SQL Scripts

## Overview

This includes SQL scripts to create two cohorts: `PVI_EarlyPVI (ID#664)` and `LowValuePVI_EarlyPVI_cohort (ID#665)`.

## How to Use

1. **Update Schema Names**: In the SQL scripts, change the following placeholders to match your database:
    - `@vocabulary_database_schema`
    - `@cdm_database_schema`
    - `@target_database_schema`

2. **Set Cohort IDs**:
    - Use `@target_cohort_id = 664` for `PVI_EarlyPVI` (or choose any ID you prefer)
    - Use `@target_cohort_id = 665` for `LowValuePVI_EarlyPVI_cohort` (or choose any ID you prefer).

3. **Run the SQL Scripts**: Execute the SQL queries to create the cohorts.

4. **Analyze in R**: Use the cohort IDs in the R scripts for analysis (Table 1 and 2)

## Support

If you have any questions, feel free to reach out at hlee110123@gmail.com
