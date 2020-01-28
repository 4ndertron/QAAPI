SELECT
    contact_id
--   , evaluation_id
FROM
    d_post_install.temp_calabrio_t_qa_evaluations
WHERE
    evaluator_id IS NULL
;