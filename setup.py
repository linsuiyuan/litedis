from setuptools import setup, find_packages

setup(
    name='litedis',  # 替换为你的包名
    version='0.1.0',  # 替换为你的版本号
    author='Your Name',  # 替换为你的名字
    author_email='your_email@example.com',  # 替换为你的邮箱
    description='A brief description of your package',  # 替换为你的包的描述
    # long_description=open('README.md').read(),  # 从 README 文件中读取长描述
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/your_package_name',  # 替换为你的项目网址
    packages=find_packages(),  # 自动找到包
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
    install_requires=[
    ],
)