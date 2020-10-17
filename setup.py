from setuptools import setup, find_packages

setup(
	name='stream-tools',
	version='0.1',
	author='Mattia Terenzi',
	packages=find_packages(),
	install_requires=[
		'aioredis==1.3.1',
		'uvloop==0.14.0'
	],
	extra_require={
		'dev': [
			'pytest',
			'flake8'
		]
	},
	entry_points={
		'console_scripts': [
			# 'stream-tools='
		]
	}
)
