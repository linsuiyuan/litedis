from setuptools import setup, find_packages

setup(
    name='litedis',
    version='3.0.5',
    author='Linsuiyuan',
    author_email='linsuiyuan@icloud.com',
    description='Litedis 是一个类似 Redis 的轻量级的、本地的、开箱即用的 NoSQL 数据库',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/linsuiyuan/litedis',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        "sortedcontainers"
    ],
)
