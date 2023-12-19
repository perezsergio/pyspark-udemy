## Spark ML Library

You are limited to the built-in models.
However they are good enough for pretty much anything outside of neural networks.

It has useful transformers like encoders and imputer, 
as well as functions to compute basic stats like correlation coefficient, 
chi-squared, ...
For regression/classification it has linreg, SVM, naive bayes, decision trees, 
knn, ...
It also has principal component analysis, singular value decomposition,
and k-means clustering for unsupervised learning.
Finally it has an implementation of the alternating least squares model
for recommendations.

These built-in models are very useful because it's not easy to parallelize 
most of these algorithms.

There are 2 machine learning libraries: MLLib (RDD api) and ML (DF api). 
You should use the newer Ml library with Dfs, MLLib is deprecated.

