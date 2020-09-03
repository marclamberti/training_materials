SELECT MIN(accuracy) < {{ var.value.avocado_dag_accuracy_threshold }}
FROM ml.accuracies 
WHERE ml_date='{{ ds }}'