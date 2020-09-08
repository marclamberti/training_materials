DELETE 
FROM xcom 
WHERE dag_id='avocado_dag' OR dag_id='avocado_dag.training_model_tasks'
AND execution_date='{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z") }}'