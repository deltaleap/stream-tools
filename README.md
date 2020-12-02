# Stream Tools
![Actions](https://github.com/deltaleap/stream-tools/workflows/Testing/badge.svg)
[![codecov](https://codecov.io/gh/deltaleap/stream-tools/branch/main/graph/badge.svg?token=AALW1P6ZZH)](https://codecov.io/gh/deltaleap/stream-tools)
[![PyPI version](https://badge.fury.io/py/stream-tools.svg)](https://badge.fury.io/py/stream-tools)
[![DeepSource](https://deepsource.io/gh/deltaleap/stream-tools.svg/?label=active+issues&show_trend=true)](https://deepsource.io/gh/deltaleap/stream-tools/?ref=repository-badge)
[![docs](https://readthedocs.org/projects/stream-tools/badge/?version=latest&style=flat)](https://stream-tools.readthedocs.io/en/latest/)

`stream-tools` is a python module that implement some utils to work with Redis Streams.
`stream-tools` aims to help building scripts to read, transform and write data in near-real-time.


### How to install

Simply type:
```
pip install stream-tools
```


### How to use

You have the following choices:

* read the [documentation](https://stream-tools.readthedocs.io/en/latest/)
* check the [examples folder](https://github.com/deltaleap/stream-tools/tree/main/examples)


### TODO

* bars (non-real-time) -> time reduction
	* sum
		* implementation
		* docs
	* average
		* implementation
		* docs
	* weighted_average
		* implementation
		* docs
	* last
		* implementation
		* docs
	* absolute_delta
		* implementation
		* docs
	* relative_delta
		* implementation
		* docs
* filters (real-time) -> dimensionality reduction
	* esponential smooth
		* implementation
		* docs
	* recursive least squares
		* implementation
		* docs
* lagger?
	* implementation
	* docs
* decorator?
	* implementation
	* docs
