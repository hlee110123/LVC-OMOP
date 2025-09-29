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
# Comprehensive query to get ALL patients with their outcomes (or NULL if no outcomes)
all_patients_df <- dbGetQuery(conn, "WITH cohort_patients AS (
    SELECT 
        subject_id as person_id,
        cohort_start_date, 
        CASE 
            WHEN cohort_definition_id = 664 THEN 'Early PVI'
            WHEN cohort_definition_id = 665 THEN 'Non-Early PVI'
        END as cohort_group,
        DATE_ADD(cohort_start_date, 183) as outcome_start_date
    FROM @cdm_database_schema.cohort 
    WHERE cohort_definition_id IN (664, 665) --CHANGE THIS: Replace 664 with your Early PVI ID and 665 with your Non-Early PVI ID 
),

clti_events AS (
    SELECT 
        cp.person_id,
        MIN(co.condition_start_date) as clti_date
    FROM cohort_patients cp
    LEFT JOIN @cdm_database_schemacondition_occurrence co 
        ON cp.person_id = co.person_id
    WHERE co.condition_concept_id IN (
        4326561,318712,443729,321052,4325344,321822,605283,605282,4141106,444264
    )
    AND co.condition_start_date > cp.outcome_start_date
    GROUP BY cp.person_id
),

pvi_events AS (
    SELECT 
        cp.person_id,
        MIN(event_date) as pvi_date
    FROM cohort_patients cp
    LEFT JOIN (
        SELECT person_id, procedure_date as event_date
        FROM @cdm_database_schemaprocedure_occurrence
        WHERE procedure_concept_id IN (
            40756934,40756856,40756946,2107761,2107774,40756958,2107775,40756872,
            2731828,2107741,40757029,2732487,40757057,2732271,2732478,2732469,
            2732496,2732505,2732514,2731567,2733065,2732523,2732728,2732719,
            2107752,2732225,2732031,40757117,2731573,2732261,2732279,2809385,
            2732270,2868339,2732252,2804374,2854686,2732253,2732262,2108036,
            2107754,2732226,2732269,2732278,2732582,2732217,2731835,2732268,
            2737032,40757128,2732591,2107760,2732486,2737041,2737513,2732224,
            2732755,2732251,2732477,2731857,
            1126206,3190814,3654286,4050289,4052406,4100317,4106321,4176449,
            4181610,4184298,4190630,4194238,4196976,4239323,4257823,4280522,
            4289770,4297862,4303275,4306749,4330921,36714397,40756783,43530797,
            43531444,44790500,45769209,46271002,46271897
        )
        UNION ALL
        SELECT person_id, device_exposure_start_date as event_date
        FROM @cdm_database_schemadevice_exposure
        WHERE device_concept_id IN (2615752,2615751,2615864)
    ) combined_events ON cp.person_id = combined_events.person_id
    WHERE event_date > cp.outcome_start_date
    GROUP BY cp.person_id
),

amputation_events AS (
    SELECT 
        cp.person_id,
        MIN(event_date) as amputation_date
    FROM cohort_patients cp
    LEFT JOIN (
        -- Amputation procedures
        SELECT person_id, procedure_date as event_date
        FROM @cdm_database_schemaprocedure_occurrence
        WHERE procedure_concept_id IN (
            4217482,4002166,3216236,3332986,4054498,3229010,37109895,3369581,36675624,
            36675625,3292024,37109581,37109845,3282074,37115743,36717437,3242376,
            3288670,3387945,3353030,3229607,4219032,37109582,3300332,36715397,3340047,
            37118455,3176310,36715395,3393829,3282425,3241791,2105451,3338258,3315344,
            4177620,3186706,37204044,3418768,4226945,3529146,3529147,3101815,3086020,
            3118156,3149876,3101809,3081993,3157298,3118164,3118158,3157573,3159869,
            3159870,3567436,3537021,3531662,4107439,4108566,4078401,3101819,40570136,
            40527679,40309384,40383478,40309388,3118159,40348021,40309376,4195136,
            4264289,36675618,4338257,4266202,4108565,4159766,4302020,4272232,2105446,
            2105448,2105449,2105209,2105211,2105222, 
            619438,619439,619440,619441,619463,619464,619465,619466,764202,764203,
            764235,764236,764237,764239,764240,764314,764315,765623,1244506,2006242,
            2006243,2103794,2104036,2104037,2104038,2104040,2104352,2104355,2104356,
            2104359,2104719,2104720,2104721,2105210,2105211,2105222,2105223,2105447,
            2105448,2105449,2105450,2105451,2105806,2109161,2109915,2110149,2110494,
            2719984,2719985,2719990,2719994,2720173,2720174,2720179,2720180,2720182,
            2720183,2720184,2720194,2720196,2720319,2720320,2720323,2720324,2720327,
            2720328,2783909,2783910,2783911,2783912,2783913,2783914,2783915,2783916,
            2783917,2783918,2783919,2783920,2783921,2783922,2783923,2783924,2783925,
            2783926,2783927,2783928,2783929,2783930,2783931,2783932,2783933,2783934,
            2783935,2783936,2783937,2783938,2783939,2783940,2783941,2783942,2783943,
            2783944,2783945,2783946,2783947,2783948,2783949,2783950,2784143,2784144,
            2784145,2784146,2784147,2784148,2784149,2784150,2784151,2784152,2784153,
            2784154,2784155,2784156,2784157,2784158,2784159,2784160,2784161,2784162,
            2784163,2784164,2784165,2784166,2784167,2784168,2784169,2784170,2784171,
            2784172,2784173,2784174,2784175,2784176,2784177,2784178,2784179,2784180,
            2784239,2784240,2784241,2784242,2784243,2784244,2784245,2784246,2784247,
            2784248,2784249,2784250,2784251,2784252,2784253,2784254,2784255,2784256,
            2784257,2784258,2784259,2784260,2784261,2784262,2784263,2784264,2784265,
            2784266,2784267,2784268,2784269,2784464,2784465,2784466,2784467,2784468,
            2784469,2784470,2784471,2784472,2784473,2784474,2784475,2784476,2784477,
            2784478,2784479,2784480,2784481,2784482,2784483,2784484,2784485,2784486,
            2784487,2784488,2784489,2784490,2784491,2784492,2784493,2784494,2784495,
            2784496,2784497,2784498,2784499,2784500,2784501,2784502,2784503,2784504,
            2784505,2784506,2784507,2784508,2784509,2784510,2784511,2784512,2784513,
            3168091,3170388,3173821,3174572,3181155,3182628,3185579,3186706,3187877,
            4002166,4008397,4010112,4020884,4030398,4034321,4034322,4034323,4034324,
            4034325,4034326,4034543,4034544,4034545,4034546,4034547,4034548,4034549,
            4034551,4034801,4034802,4034803,4034804,4035025,4035764,4039429,4040995,
            4049802,4050720,4052082,4053140,4054498,4054983,4057890,4062385,4067621,
            4072584,4074138,4078558,4078560,4078561,4078562,4078563,4078576,4080100,
            4083394,4083396,4083397,4083398,4085088,4106037,4108565,4108567,4116562,
            4116915,4119910,4132734,4133831,4139581,4143795,4143796,4143797,4146410,
            4147118,4148305,4151354,4159766,4165043,4167170,4169822,4177620,4183102,
            4195136,4196243,4196938,4200549,4204692,4208330,4216997,4217266,4217482,
            4217608,4218050,4219032,4219468,4223824,4224928,4226945,4227157,4228939,
            4230372,4231940,4234959,4235002,4242396,4244214,4245865,4246689,4246691,
            4249167,4262531,4263139,4263590,4264278,4264289,4264290,4264716,4266202,
            4271020,4272232,4282910,4283655,4284672,4287928,4294385,4297321,4299365,
            4300370,4302020,4306195,4306196,4306760,4308894,4313420,4323971,4338257,
            35622529,36674416,36674417,36675618,36675624,36675625,36715395,36715396,
            36715397,36717437,37109581,37109582,37109583,37109584,37109822,37109845,
            37109850,37109895,37110112,37110113,37110114,37110200,37110201,37115742,
            37115743,37116276,37117184,37118034,37118455,37150959,37160759,37168055,
            37168056,37168057,37168064,37168566,37168607,37172247,37172374,37174088,
            37204044,37206818,37206819,37206860,37206861,40217611,40217613,40479688,
            40480578,40484035,40486729,40486730
        )
        UNION ALL
        -- Amputation-related devices (prosthetics, etc.)
        SELECT person_id, device_exposure_start_date as event_date
        FROM @cdm_database_schemadevice_exposure
        WHERE device_concept_id IN (
            2719984, 2719985, 2719994, 2720173, 2720174, 2720179, 2720180, 2720182, 2720183, 2720184,
            2720194, 2720196, 2720319, 2720320, 2720323, 2720324, 2720327, 2720328
        )
    ) combined_amp_events ON cp.person_id = combined_amp_events.person_id
    WHERE event_date > cp.outcome_start_date
    GROUP BY cp.person_id
)

SELECT 
    cp.person_id,
    cp.cohort_group,
    cp.cohort_start_date as index_date,
    cp.outcome_start_date,
    COALESCE(d.death_date, '2022-12-31') as end_date,
    DATEDIFF(COALESCE(d.death_date, '2022-12-31'), cp.cohort_start_date) as follow_up_days,
    
    clti.clti_date,
    CASE WHEN clti.clti_date IS NOT NULL THEN 1 ELSE 0 END as had_clti,
    CASE WHEN clti.clti_date IS NOT NULL 
         THEN DATEDIFF(clti.clti_date, cp.cohort_start_date) 
         ELSE NULL 
    END as days_to_clti,
    
    pvi.pvi_date,
    CASE WHEN pvi.pvi_date IS NOT NULL THEN 1 ELSE 0 END as had_pvi,
    CASE WHEN pvi.pvi_date IS NOT NULL 
         THEN DATEDIFF(pvi.pvi_date, cp.cohort_start_date) 
         ELSE NULL 
    END as days_to_pvi,
    
    amp.amputation_date,
    CASE WHEN amp.amputation_date IS NOT NULL THEN 1 ELSE 0 END as had_amputation,
    CASE WHEN amp.amputation_date IS NOT NULL 
         THEN DATEDIFF(amp.amputation_date, cp.cohort_start_date) 
         ELSE NULL 
    END as days_to_amputation
FROM cohort_patients cp
LEFT JOIN @cdm_database_schemadeath d ON cp.person_id = d.person_id
LEFT JOIN clti_events clti ON cp.person_id = clti.person_id
LEFT JOIN pvi_events pvi ON cp.person_id = pvi.person_id
LEFT JOIN amputation_events amp ON cp.person_id = amp.person_id
")

nrow(all_patients_df)

#--------------------- Part III: Analysis ---------------------
# Filter all_patients_df to include only person_ids from filtered_data
all_patients_outcomes <- all_patients_df[all_patients_df$person_id %in% filtered_data$person_id, ]

# Prepare the data with proper labels
all_patients_outcomes <- all_patients_outcomes %>%
  mutate(
    # Create group variable (assuming cohort_group has values 0/1 or text)
    Group = case_when(
      cohort_group == 1 | cohort_group == "Early PVI" ~ "Early_PVI",
      cohort_group == 0 | cohort_group == "Non-Early PVI" ~ "Non_Early_PVI",
      TRUE ~ as.character(cohort_group)
    ),
    Group = factor(Group, levels = c("Non_Early_PVI", "Early_PVI"))
  ) %>%
  # Convert binary outcomes to factors
  mutate(
    CLTI = factor(had_clti, levels = c(0, 1), labels = c("No", "Yes")),
    Repeat_PVI = factor(had_pvi, levels = c(0, 1), labels = c("No", "Yes")),
    Major_Amputation = factor(had_amputation, levels = c(0, 1), labels = c("No", "Yes"))
  )

# Create the comprehensive analysis table
table2_df <- mytable(cohort_group ~ 
                           follow_up_days + 
                           CLTI + 
                           days_to_clti + 
                           Repeat_PVI + 
                           days_to_pvi + 
                           Major_Amputation + 
                           days_to_amputation, 
                         data = all_patients_outcomes)

# Display results
print(table2_df)

# Save as CSV
mycsv(table2_df, file = "LVC_Table2.csv")
