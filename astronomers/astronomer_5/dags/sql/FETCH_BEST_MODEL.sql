SELECT ml_id 
FROM ml.accuracies 
WHERE accuracy = (
    SELECT MIN(accuracy)
    FROM ml.accuracies 
    WHERE ml_date='{{ ds }}'
)
AND ml_date='{{ ds }}'