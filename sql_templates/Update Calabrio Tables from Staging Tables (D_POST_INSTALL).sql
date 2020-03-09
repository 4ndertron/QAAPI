-- 1) Evaluation Forms: T_QA_FORMS
-- ===============================
-- todo: Integrate the following query commands into the api handler.

-- A) Full replace
CREATE OR REPLACE TABLE d_post_install.temp_calabrio_t_qa_forms COPY GRANTS AS
SELECT ef.src:id::NUMBER       AS form_id
     , ef.src: name::VARCHAR   AS form_name
     , es.value:id::NUMBER     AS section_id
     , es.value: name::VARCHAR AS section_name
     , es.value:weight::FLOAT  AS section_weight
     , eq.value:id::NUMBER     AS question_id
     , eq.value: text::VARCHAR AS question_text
     , eq.value:weight::FLOAT  AS question_weight
     , eo.value:id::NUMBER     AS option_id
     , eo.value:label::VARCHAR AS option_label
     , eo.value:points::NUMBER AS option_points
     , eo.value: type::VARCHAR AS option_type
FROM d_post_install.temp_calabrio_t_qa_forms_staging AS ef
   , LATERAL FLATTEN(ef.src:sections) es
   , LATERAL FLATTEN(es.value:questions) eq
   , LATERAL FLATTEN(eq.value:options) eo
ORDER BY form_id
       , section_id
       , question_id
       , option_id
;



-- 2) Contacts: T_QA_CONTACTS
-- ==========================

-- A) MERGE on contact_id
-- i) INSERT non-matches
MERGE INTO
    d_post_install.temp_calabrio_t_qa_contacts AS qc
    USING
        (
            SELECT co.src:id::NUMBER                                                     AS contact_id
                 , CONVERT_TIMEZONE('UTC', 'America/Denver',
                                    DATEADD(ms, co.src:startTime::NUMBER, '1970-01-01')) AS contact_start_time
                 , 'https://calabriocloud.com/index.html#/recordings/' || co.src:id ||
                   '/ccr'                                                                AS contact_url
                 , co.src:assocCallId::VARCHAR                                           AS cjp_session_id
            FROM d_post_install.temp_calabrio_t_qa_contacts_staging AS co
            ORDER BY contact_start_time
        ) AS new
    ON
        qc.contact_id = new.contact_id
    WHEN NOT MATCHED THEN
        INSERT VALUES (new.contact_id, new.contact_start_time, new.contact_url, new.cjp_session_id)
;



-- 2.5) All Contacts: T_CONTACTS
-- =============================

-- A) MERGE on contact_id
--     i) INSERT non-matches
MERGE INTO
    d_post_install.temp_calabrio_t_contacts AS cc
    USING
        (
            SELECT co.src:id::NUMBER                                                                              AS contact_id
                 , CONVERT_TIMEZONE('UTC', 'America/Denver',
                                    DATEADD(ms, co.src:startTime::NUMBER, '1970-01-01'))                          AS contact_start_time
                 , 'https://calabriocloud.com/index.html#/recordings/' || co.src:id ||
                   '/ccr'                                                                                         AS contact_url
                 , co.src:assocCallId::VARCHAR                                                                    AS cjp_session_id
            FROM d_post_install.temp_calabrio_t_contacts_staging AS co
            ORDER BY contact_start_time
        ) AS new
    ON
        cc.contact_id = new.contact_id
    WHEN NOT MATCHED THEN
        INSERT VALUES (new.contact_id, new.contact_start_time, new.contact_url, new.cjp_session_id)
;



-- 3) Evaluations: T_QA_EVALUATIONS
-- ================================
-- DELETE then MERGE to address the possibility that an evaluation was deleted.

-- A) DELETE from T_QA_EVALUATIONS where contact_id is in T_QA_CONTACTS_STAGING and evaluation_id is not in T_QA_EVALUATIONS_STAGING
DELETE
FROM d_post_install.temp_calabrio_t_qa_evaluations
WHERE contact_id IN (SELECT src:id::NUMBER FROM d_post_install.temp_calabrio_t_qa_contacts_staging)
  AND NOT evaluation_id IN (SELECT src:id::NUMBER FROM d_post_install.temp_calabrio_t_qa_evaluations_staging)
;

-- B) MERGE on evaluation_id
-- i) UPDATE matches
-- ii) INSERT non-matches
MERGE INTO
    d_post_install.temp_calabrio_t_qa_evaluations AS qe
    USING
        (
            SELECT ev.src:id::NUMBER                                                     AS evaluation_id
                 , ev.src:evalForm:evalFormId::NUMBER                                    AS form_id
                 , REGEXP_SUBSTR(ev.src:qualityRef, '\\d+$')::NUMBER                     AS contact_id
                 , ev.src:agent:id::NUMBER                                               AS agent_id
                 , ev.src:evaluator:id::NUMBER                                           AS evaluator_id
                 , IFF(ev.src:isScoreCounted::BOOLEAN, 'Evaluation', 'Calibration')      AS eval_type
                 -- Convert from UTC to America/Denver time (evaluatedTz is a field, but it just records which timezone Calabrio believes the evaluator is in; ce.src:evaluatedTz::VARCHAR is INCORRECT)
                 , CONVERT_TIMEZONE('UTC', 'America/Denver',
                                    DATEADD(ms, ev.src:evaluated::NUMBER, '1970-01-01')) AS evaluated_date
                 , ev.src:responseState: text::VARCHAR                                   AS response_state
                 -- raw_score: ADDITIVE scores only
                 -- final_score: same as raw, but 0 if they failed any KPI questions
                 , ev.src:additiveScore::NUMBER                                          AS raw_score
                 , ev.src:totalScore::FLOAT                                              AS final_score
            FROM d_post_install.temp_calabrio_t_qa_evaluations_staging AS ev
            WHERE
                -- Keep only completed evaluations
                ev.src:state: text = 'SCORED'
                QUALIFY
                    -- Keep only one row per evaluation
                    ROW_NUMBER() OVER (PARTITION BY evaluation_id ORDER BY 1) = 1
            ORDER BY contact_id
                   , evaluation_id
        ) AS new
    ON
        qe.evaluation_id = new.evaluation_id
    WHEN MATCHED THEN
        -- evaluation_id and contact_id should never change
        -- Keep old evaluated_date
        UPDATE SET form_id = new.form_id, agent_id = new.agent_id, evaluator_id = new.evaluator_id, eval_type = new.eval_type, response_state = new.response_state, raw_score = new.raw_score, final_score = new.final_score
    WHEN NOT MATCHED THEN
        INSERT VALUES (new.evaluation_id, new.form_id, new.contact_id, new.agent_id, new.evaluator_id, new.eval_type,
                       new.evaluated_date, new.response_state, new.raw_score, new.final_score)
;



-- 4) Evaluation Scores: T_QA_EVALUATION_SCORES
-- ============================================
-- DELETE then INSERT instead of UPDATE/MERGE to address the possibility that an evaluation was deleted.

-- A) DELETE from T_QA_EVALUATION_SCORES where contact_id is in T_QA_CONTACTS_STAGING
DELETE
FROM d_post_install.temp_calabrio_t_qa_evaluation_scores
WHERE contact_id IN (SELECT src:id::NUMBER FROM d_post_install.temp_calabrio_t_qa_contacts_staging)
;

-- B) INSERT from T_QA_EVALUATIONS_STAGING
INSERT INTO d_post_install.temp_calabrio_t_qa_evaluation_scores
    (
        SELECT ev.src:id::NUMBER                                AS evaluation_id
             , REGEXP_SUBSTR(ev.src:qualityRef, '\\d+')::NUMBER AS contact_id
             , es.value:id::NUMBER                              AS section_id
             , eq.value:id::NUMBER                              AS question_id
             , eq.value:selectedOption::NUMBER                  AS option_id
        FROM d_post_install.temp_calabrio_t_qa_evaluations_staging AS ev
           -- LATERAL FLATTEN removes rows where the flattened field contains nothing.
           -- This is okay because:
           -- A) There aren't any rows that get removed in this case
           -- B) We're specifically looking for questions anyway (so if any evaluations were thrown out, it would be okay)
           , LATERAL FLATTEN(ev.src:sections) es
           , LATERAL FLATTEN(es.value:questions) eq
        WHERE
            -- Keep only completed evaluations
            ev.src:state: text = 'SCORED'
        ORDER BY evaluation_id
               , section_id
               , question_id
    )
;



-- 5) Evaluation Comments: T_QA_EVALUATION_COMMENTS
-- ================================================
-- DELETE then INSERT instead of UPDATE/MERGE to address the possibility that a comment was deleted.

-- A) DELETE from T_QA_EVALUATION_COMMENTS where contact_id is in T_QA_CONTACTS_STAGING
DELETE
FROM d_post_install.temp_calabrio_t_qa_evaluation_comments
WHERE contact_id IN (SELECT src:id::NUMBER FROM d_post_install.temp_calabrio_t_qa_contacts_staging)
;

-- B) INSERT from T_QA_EVALUATION_COMMENTS_STAGING
INSERT INTO d_post_install.temp_calabrio_t_qa_evaluation_comments
    (
        SELECT REGEXP_SUBSTR(co.src:"$ref", '\\d+', 1, 3)::NUMBER               AS comment_id
             , REGEXP_SUBSTR(co.src:"$ref", '\\d+', 1, 1)::NUMBER               AS contact_id
             , REGEXP_SUBSTR(co.src:"$ref", '\\d+', 1, 2)::NUMBER               AS evaluation_id
             -- Section ID needed because there can be section-level comments (non-NULL section_id with NULL question_id)
             , co.src:sectionFK::NUMBER                                         AS section_id
             , co.src:questionFK::NUMBER                                        AS question_id
             -- Convert from UTC to America/Denver time (tz is a field, but it just records which timezone Calabrio believes the evaluator is in; ce.src:tz::VARCHAR is INCORRECT)
             -- If the comment has ever been updated, the update times are listed in src:history (so [created] and [commentor] from [history] should be prioritized)
             , CONVERT_TIMEZONE('UTC', 'America/Denver', DATEADD(ms, NVL(ch.value:created, co.src:created)::NUMBER,
                                                                 '1970-01-01')) AS created_date
             , REGEXP_SUBSTR(NVL(ch.value:commentor:"$ref", co.src:commentor:"$ref"),
                             '\\d+')::NUMBER                                    AS commentor_id
             , co.src: text::VARCHAR                                            AS text
        FROM d_post_install.temp_calabrio_t_qa_evaluation_comments_staging AS co
           -- Have to change '[]' to '[{}]' to avoid losing rows with empty values in the history field
           , LATERAL FLATTEN(IFF(co.src: history = '[]', PARSE_JSON('[{}]'), co.src: history)::VARIANT) AS ch
        WHERE
            -- Require comment to contain at least one alphanumeric character
            REGEXP_LIKE(text, '^.*[[:alnum:]].*$', 's')
            QUALIFY
                -- Keep only the row with the newest date per comment (there will only be one row if there is no date in history)
                    ROW_NUMBER() OVER (PARTITION BY comment_id ORDER BY ch.value:created DESC) = 1
        ORDER BY contact_id
               , evaluation_id
               , section_id
               , question_id
               , created_date
    )
;

MERGE INTO
    d_post_install.temp_calabrio_t_contacts_staging_backup AS cb
    USING
        d_post_install.temp_calabrio_t_contacts_staging AS cs
    ON
        cb.src:id::VARCHAR = cs.src:id::VARCHAR
    WHEN MATCHED THEN
        UPDATE SET src = cs.src
    WHEN NOT MATCHED THEN
        INSERT VALUES (cs.src)
;