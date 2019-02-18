FROM sd2e/reactors:python3-edge

RUN pip uninstall --yes datacatalog
# COPY datacatalog /datacatalog

RUN pip3 install git+https://github.com/SD2E/python-datacatalog.git@v1.0.0
