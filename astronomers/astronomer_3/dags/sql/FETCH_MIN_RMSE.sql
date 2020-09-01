SELECT MIN(accuracy) < 0.15
FROM ml.accuracies 
WHERE ml_date='2020-08-02'