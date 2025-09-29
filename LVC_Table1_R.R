# Import package
library(rJava)
library(DatabaseConnector)
library(writexl)
library(readr)
library(moonBook)
library(dplyr)

#--------------------- Part I: Database Connection ---------------------
connectionDetails <- createConnectionDetails(dbms = "your dbms",
                                             connectionString = "your jdbc driver",
                                             pathToDriver = "your pathToDriver")

conn <- connect(connectionDetails)

#--------------------- Part II: Covariate generation ---------------------
# 2-1. Early PVI Comorbidities
comor_earlypvi <- dbGetQuery(conn, "SELECT  
                             b.subject_id AS person_id, 
                             b.cohort_start_date, 
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (46270483,4196141,4175440,37016767,37016768,376979,4225656,4221495,43531578,4338897,442793,45769832,43531616,201820,4008576,4033942,4159742,4009303,4048028,4042502,443767,42538169,443733,192279,443730,
                                                               377821,376065,43530690,4226354,4223303,4222876,4214376,37016348,37016349,4226238,201530,4032787,4029423,45769876,45757363,4226798,4228112,36714116,4095288,4224254,4228443,4191611,4143857,4140466,
                                                               45770830,380097,378743,37016179,45757435,377552,37016180,45770881,4225055,4222415,4114427,45769830,43531563,4044391,45763583,43530656,134398,4131908,318712,443729,321822,376112,37017431,37017432,
                                                               380096,45763584,43530685,200687,443731,4174977,4227210,376114,4290822,4266637,4227657,4338901,45769873,45773064,4338896,201254,4152858,4099214,443412,201826,4099651,4193704,45770902) 
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 
                               ELSE 0 
                               END
                             ) AS diabetes,
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (44782429,320128,4110948,44784621,439696,319034,444101,443919,443771,317895,319826) 
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 ELSE 0 
                               END
                             ) AS hypertension,
                             MAX(
                               CASE 
                               WHEN (
                                 (o.observation_concept_id IN (4005823, 4276526, 4218741) 
                                  AND o.observation_date < b.cohort_start_date - INTERVAL 365 DAY)
                                 OR
                                 (a.condition_concept_id IN (4103418, 4146763, 4233811, 4209423 ) 
                                   AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY)
                               ) THEN 1 ELSE 0 
                               END
                             ) AS smoking,
                             MAX(
                               CASE 
                               WHEN (
                                 (a.condition_concept_id IN (46271022, 44782429, 43531578, 443597, 45763854, 45763855, 
                                                             443612, 443611, 4019967, 193782, 443919, 192359, 201826) 
                                  AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY)
                                 OR
                                 (o.observation_concept_id = 4019967 
                                   AND o.observation_date < b.cohort_start_date - INTERVAL 365 DAY)
                               ) THEN 1 ELSE 0 
                               END
                             ) AS eskd,
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (256451, 255841, 255841, 261325, 255573, 256449)
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 ELSE 0 
                               END
                             ) AS copd,
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (443614, 443601, 443597, 45763854, 45763855, 443612, 443611, 443611, 44782429, 201826, 43531578, 4140207)
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 ELSE 0 
                               END
                             ) AS chronic_kidney_disease,
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (321318, 312327, 4108217, 4176969, 315286)
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 ELSE 0 
                               END
                             ) AS ischemic_heart_disease,
                             MAX(
                               CASE 
                               WHEN a.condition_concept_id IN (316139)
                               AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
                               THEN 1 ELSE 0 
                               END
                             ) AS congestive_heart_failure
                             FROM 
                             @cdm_database_schema.cohort b
                             LEFT JOIN 
                             @cdm_database_schema.condition_occurrence a
                             ON 
                             b.subject_id = a.person_id
                             LEFT JOIN 
                             @cdm_database_schema.observation o
                             ON 
                             b.subject_id = o.person_id
                             WHERE 
                             b.cohort_definition_id = 664
                             GROUP BY 
                             b.subject_id, b.cohort_start_date
                             ")

# 2-2. Non Early PVI Comorbidities
comor_nonearlypvi <- dbGetQuery(conn, "SELECT  
    b.subject_id AS person_id, 
    b.cohort_start_date, 
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (46270483,4196141,4175440,37016767,37016768,376979,4225656,4221495,43531578,4338897,442793,45769832,43531616,201820,4008576,4033942,4159742,4009303,4048028,4042502,443767,42538169,443733,192279,443730,
            377821,376065,43530690,4226354,4223303,4222876,4214376,37016348,37016349,4226238,201530,4032787,4029423,45769876,45757363,4226798,4228112,36714116,4095288,4224254,4228443,4191611,4143857,4140466,
            45770830,380097,378743,37016179,45757435,377552,37016180,45770881,4225055,4222415,4114427,45769830,43531563,4044391,45763583,43530656,134398,4131908,318712,443729,321822,376112,37017431,37017432,
            380096,45763584,43530685,200687,443731,4174977,4227210,376114,4290822,4266637,4227657,4338901,45769873,45773064,4338896,201254,4152858,4099214,443412,201826,4099651,4193704,45770902) 
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 
            ELSE 0 
        END
    ) AS diabetes,
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (44782429,320128,4110948,44784621,439696,319034,444101,443919,443771,317895,319826) 
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 ELSE 0 
        END
    ) AS hypertension,
    MAX(
        CASE 
            WHEN (
                (o.observation_concept_id IN (4005823, 4276526, 4218741) 
                 AND o.observation_date < b.cohort_start_date - INTERVAL 365 DAY)
                OR
                (a.condition_concept_id IN (4103418, 4146763, 4233811, 4209423 ) 
                 AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY)
            ) THEN 1 ELSE 0 
        END
    ) AS smoking,
    MAX(
        CASE 
            WHEN (
                (a.condition_concept_id IN (46271022, 44782429, 43531578, 443597, 45763854, 45763855, 
                                            443612, 443611, 4019967, 193782, 443919, 192359, 201826) 
                 AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY)
                OR
                (o.observation_concept_id = 4019967 
                 AND o.observation_date < b.cohort_start_date - INTERVAL 365 DAY)
            ) THEN 1 ELSE 0 
        END
    ) AS eskd,
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (256451, 255841, 255841, 261325, 255573, 256449)
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 ELSE 0 
        END
    ) AS copd,
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (443614, 443601, 443597, 45763854, 45763855, 443612, 443611, 443611, 44782429, 201826, 43531578, 4140207)
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 ELSE 0 
        END
    ) AS chronic_kidney_disease,
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (321318, 312327, 4108217, 4176969, 315286)
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 ELSE 0 
        END
    ) AS ischemic_heart_disease,
    MAX(
        CASE 
            WHEN a.condition_concept_id IN (316139)
            AND a.condition_start_date < b.cohort_start_date - INTERVAL 365 DAY 
            THEN 1 ELSE 0 
        END
    ) AS congestive_heart_failure
FROM 
    @cdm_database_schema.cohort b
LEFT JOIN 
    @cdm_database_schema.condition_occurrence a
ON 
    b.subject_id = a.person_id
LEFT JOIN 
    @cdm_database_schema.observation o
ON 
    b.subject_id = o.person_id
WHERE 
    b.cohort_definition_id = 665
GROUP BY 
    b.subject_id, b.cohort_start_date
")

# 2-3. Early PVI Demographics
demo_earlypvi <- dbGetQuery(conn,"SELECT 
                             a.person_id, 
                             (EXTRACT(YEAR FROM b.cohort_start_date) - a.year_of_birth) AS age, 
                             a.gender_concept_id, 
                             a.race_concept_id, 
                             a.ethnicity_concept_id
                             FROM 
                             @cdm_database_schema.person a
                             LEFT JOIN 
                             @cdm_database_schema.cohort b
                             ON 
                             b.subject_id = a.person_id
                             WHERE 
                             b.cohort_definition_id = 664")

# 2-4. Non Early PVI Demographics
demo_nonearlypvi <- dbGetQuery(conn,"SELECT 
                             a.person_id, 
                             (EXTRACT(YEAR FROM b.cohort_start_date) - a.year_of_birth) AS age, 
                             a.gender_concept_id, 
                             a.race_concept_id, 
                             a.ethnicity_concept_id
                             FROM 
                             @cdm_database_schema.person a
                             LEFT JOIN 
                             @cdm_database_schema.cohort b
                             ON 
                             b.subject_id = a.person_id
                             WHERE 
                             b.cohort_definition_id = 665")


# Merge demographics and comorbidities for two groups
demo_earlypvi_df <- as.data.frame(demo_earlypvi)
comor_earlypvi_df <- as.data.frame(comor_earlypvi)

demo_nonearlyEP<- as.data.frame(demo_nonearlypvi)
comor_nonearlyEP<- as.data.frame(comor_nonearlypvi)

# Check the output of the previous code execution.
# If a NameError message pops up:
# 1. Print each dataframe in a separate cell.
#    - Use: print(demo_earlypvi_df) or View(demo_earlypvi_df) for the demo dataframe.
#    - Use: print(comor_earlypvi_df) or View(comor_earlypvi_df) for the comorbidity dataframe.
# 2. Rerun the following merge command:
#    mergedEP <- merge(demo_earlypvi_df, comor_earlypvi_df, by = "person_id")
#    merged_nonEP <- merge(demo_nonearlyEP, comor_nonearlyEP, by = "person_id")

# Merging the two datasets by 'person_id' : Early PVI (ID664)
mergedEP <- merge(demo_earlypvi_df, comor_earlypvi_df, by = "person_id")

# Merging the two datasets by 'person_id' : Non early PVI (ID665)
merged_nonEP <- merge(demo_nonearlyEP, comor_nonearlyEP, by = "person_id")

#--------------------- Part III: Analysis ---------------------

# Add a column to indicate control (0) for `merged_nonep`
merged_nonEP$group <- 0

# Add a column to indicate case (1) for `merged_ep`
mergedEP$group <- 1

# Combine the two data frames into one
combined_data <- rbind(merged_nonEP, mergedEP)

# Display the combined data frame
print(combined_data)

# Filter combined_data to include only rows where:
# 1. Age is greater than or equal to 18
# 2. Gender concept id is not 0
# 3. Race concept id is not 0
# 4. Ethnicity concept id is not 0

filtered_data <- combined_data[combined_data$age >= 18 & 
                                 combined_data$gender_concept_id != 0 & 
                                 combined_data$race_concept_id != 0 &
                                 combined_data$ethnicity_concept_id != 0, ]

# Convert concept_id from integer to character (string) (IF NEEDED!)
#filtered_data$race_concept_id <- as.character(filtered_data$race_concept_id)
#filtered_data$gender_concept_id <- as.character(filtered_data$gender_concept_id)
#filtered_data$ethnicity_concept_id <- as.character(filtered_data$ethnicity_concept_id)

# Define your continuous and categorical variables
cont_variables <- c('age')
categorical_variables <- c('gender_concept_id', 'race_concept_id', 'ethnicity_concept_id',
                           'diabetes', 'hypertension', 'smoking', 'eskd', 'copd',
                           'chronic_kidney_disease', 'ischemic_heart_disease', 'congestive_heart_failure')

# Create meaningful labels for all categorical variables
filtered_data <- filtered_data %>%
  # Create labeled group variable
  mutate(
    group_labeled = case_when(
      group == 0 ~ "Non Early PVI",
      group == 1 ~ "Early PVI",
      TRUE ~ as.character(group)
    ),
    group_labeled = factor(group_labeled, levels = c("Non Early PVI", "Early PVI"))
  ) %>%
  # Label binary variables (0 = No, 1 = Yes)
  mutate(
    across(c(diabetes, hypertension, smoking, eskd, copd,
             chronic_kidney_disease, ischemic_heart_disease, 
             congestive_heart_failure), 
           ~ case_when(
             . == 0 ~ "No",
             . == 1 ~ "Yes",
             TRUE ~ as.character(.)
           ))
  ) %>%
  # Convert binary variables to factors
  mutate(
    across(c(diabetes, hypertension, smoking, eskd, copd,
             chronic_kidney_disease, ischemic_heart_disease, 
             congestive_heart_failure), 
           ~ factor(., levels = c("No", "Yes")))
  )

# Create summary table with statistical tests using moonBook
result_table <- mytable(group ~ age + gender_concept_id + race_concept_id + ethnicity_concept_id +
                          diabetes + hypertension + smoking + eskd + copd + 
                          chronic_kidney_disease + ischemic_heart_disease + 
                          congestive_heart_failure, 
                        data = filtered_data)

result_table

# Save directly as CSV using moonBook's built-in function 
mycsv(result_table, file = "LVC_table1.csv")
