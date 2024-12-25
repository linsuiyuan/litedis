import os
import shutil

from invoke import task, run

@task
def clean(c):
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")
    if os.path.isdir("litedis.egg-info"):
        shutil.rmtree("litedis.egg-info")


@task
def package(c):
    run("python setup.py sdist bdist_wheel")


@task
def push(c):
    run("git push origin main")
    run("git push github main")
    run("twine upload dist/*")
