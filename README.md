# Early PVI Cohort SQL Scripts

## Overview

This includes SQL scripts to create two cohorts: `PVI_EarlyPVI (ID#664)` and `LowValuePVI_EarlyPVI_cohort (ID#665)`.

## Step 1: Create Cohorts

### Option A: If you DO NOT have ATLAS

1. **Update Schema Names**: In the SQL scripts, change the following placeholders to match your database:
   - `@vocabulary_database_schema`
   - `@cdm_database_schema`
   - `@target_database_schema`

2. **Set Cohort IDs**: 
   - Use `@target_cohort_id = 664` for `PVI_Early PVI` (or choose any ID you prefer)
   - Use `@target_cohort_id = 665` for `PVI_Non EarlyPVI` (or choose any ID you prefer)

3. **Execute SQL Scripts**: Run the SQL scripts in your database to create the cohorts.

### Option B: If you HAVE ATLAS

1. Use ATLAS to create the cohorts and note the cohort definition IDs generated.

---

## Step 2: Generate Table 1

1. Open `LVC table 1.R`
2. Update the script with your **cohort definition ID** from Step 1
3. Run the script to generate Table 1
4. Save the output as a **CSV file**
5. **Submit the CSV file to us**

---

## Step 3: Generate Table 2

1. Open `LVC table 2.R`
2. Update the script with your **cohort definition ID** from Step 1
3. Run the script to generate covariates
4. **Important**: Filter the covariates using the same cohorts from Table 1 (`filtered_df`)
5. Save the result as a **CSV file**
6. **Submit the CSV file to us**

## Support

If you have any questions, feel free to reach out at hlee110123@gmail.com
