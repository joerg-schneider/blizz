import shutil
import subprocess
import os
import tempfile

import test
from urllib import request
from time import sleep

PROJECT_HOME_DIR = os.path.join(
    os.path.dirname(os.path.realpath(test.__file__)), os.pardir, os.pardir
)
TUTORIAL_DIR = os.path.abspath(
    os.path.join(PROJECT_HOME_DIR, "src")
)
TEST_FEATURE_GROUP_LIST = os.path.abspath(
    os.path.join(PROJECT_HOME_DIR, "src/tutorial/ExampleFeatureList.yaml")
)
TEST_FEATURE_GROUP_DIR = os.path.abspath(
    os.path.join(PROJECT_HOME_DIR, "src/tutorial/example_feature_library")
)


def test_build():
    tempdir = None
    try:
        tempdir = tempfile.mkdtemp()
        output_dir = os.path.join(tempdir, "output")
        subprocess.run(
            args=f"blizz build {TEST_FEATURE_GROUP_LIST} {TEST_FEATURE_GROUP_DIR} {output_dir}",
            shell=True,
            cwd=PROJECT_HOME_DIR,
        )
        files = os.listdir(output_dir)
        assert "StudentFeatureGroup" in files
    finally:
        if tempdir:
            shutil.rmtree(tempdir, ignore_errors=True)


def test_bootstrap():
    subprocess.run("blizz bootstrap", shell=True)


def test_docs():
    test_port = 9999
    try:
        p = subprocess.Popen(
            args=f"blizz docs {TEST_FEATURE_GROUP_DIR} --serve --port {test_port}",
            shell=True,
            cwd=PROJECT_HOME_DIR,
        )
        sleep(3)
        with request.urlopen(f"http://localhost:{test_port}", timeout=20) as req:
            assert req.status == 200

    finally:
        if p:
            p.terminate()
