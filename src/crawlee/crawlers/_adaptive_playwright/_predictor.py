
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression

X = [ [f"{i}"] for i in range(0,100)]
y = ["a"] *50 + ["b"] *50
clf = LogisticRegression(random_state=0).fit(X, y)
point = [[49],]
print(clf.predict(point))
print(clf.predict_proba(point))

