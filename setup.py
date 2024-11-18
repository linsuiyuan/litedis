from setuptools import setup, find_packages

setup(
    name='litedis',
    version='2.1.5',
    author='Linsuiyuan',
    author_email='linsuiyuan@icloud.com',
    description='Litedis 是一个轻量级的 模仿Redis 的本地实现，它实现了和 Redis 类似的功能',
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
    install_requires=[],
)
