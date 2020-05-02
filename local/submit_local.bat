
jupyter nbconvert --to script desafio2.ipynb

%SPARK_HOME%/bin/spark-submit.cmd --master local[2] desafio2.py