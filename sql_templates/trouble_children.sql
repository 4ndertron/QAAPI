-- What questions are missing?
SELECT DISTINCT
    e.contact_id
  , e.evaluation_id
  , f.section_id
  , f.question_id
FROM
    d_post_install.temp_calabrio_t_qa_forms AS f
INNER JOIN
    d_post_install.temp_calabrio_t_qa_evaluations AS e
ON
    f.form_id = e.form_id
LEFT OUTER JOIN
    d_post_install.temp_calabrio_t_qa_evaluation_scores AS s
ON
    e.evaluation_id = s.evaluation_id
AND f.question_id = s.question_id
WHERE
    s.question_id IS NULL
ORDER BY
    e.evaluation_id
  , f.section_id
  , f.question_id
;